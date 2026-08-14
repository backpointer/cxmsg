import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

test("Claude deliveries persist transport state and schedule bounded 529 retries", async () => {
  const stateDir = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-delivery-state-"));
  process.env.CXMSG_STATE_DIR = stateDir;
  try {
    const {
      createClaudeDeliveryJob,
      parseClaudeDeliveryAck,
      recordClaudeDeliveryAck,
      refreshClaudeDelivery,
      sendClaudeDeliveryJob,
    } = await import("../src/claude-delivery.js");
    const { readJob, updateJob } = await import("../src/jobs.js");
    const peer = {
      name: "reviewer",
      sessionId: "87654321-4321-4321-4321-cba987654321",
      address: "uds:/tmp/cc-socks/12345.sock",
      socketPath: "/tmp/cc-socks/12345.sock",
      status: "idle",
    };
    const sourceRecord = { threadId: "source-thread" };
    let job = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "review the change",
      maxAttempts: 3,
    });
    const frames = [];
    job = await sendClaudeDeliveryJob(
      { socketPath: "/tmp/cc-socks/99999.sock" },
      sourceRecord,
      job,
      {
        peers: async () => [peer],
        send: async (_socketPath, frame) => frames.push(frame),
      },
    );
    assert.equal(job.status, "transport_delivered");
    assert.equal(job.delivery.attempt, 1);
    assert.match(frames[0].message.content, new RegExp(job.jobId));

    const retryAck = parseClaudeDeliveryAck(
      `<cxmsg-ack in-reply-to="${job.jobId}" status="retryable_error" code="529" retry-after="1">\nOverloaded\n</cxmsg-ack>`,
    );
    job = await recordClaudeDeliveryAck(
      {
        fromSession: peer.sessionId,
        fromAddress: peer.address,
      },
      retryAck,
    );
    assert.equal(job.status, "retry_scheduled");
    assert.equal(job.ack.code, "529");
    assert.ok(job.delivery.nextAttemptAt);

    const completedAck = parseClaudeDeliveryAck(
      `<cxmsg-ack in-reply-to="${job.jobId}" status="completed">\nDone\n</cxmsg-ack>`,
    );
    job = await recordClaudeDeliveryAck(
      {
        fromSession: peer.sessionId,
        fromAddress: peer.address,
      },
      completedAck,
    );
    assert.equal(job.status, "completed");
    assert.equal(job.result, "Done");
    assert.equal(readJob(job.jobId).status, "completed");

    const { handleClaudeDeliveryAck } = await import("../src/claude-bridge.js");
    const { deliverPeerMessage } = await import("../src/messaging.js");
    const wakes = [];
    const wakeCalls = [];
    const wakeClient = {
      async request(method, params) {
        wakeCalls.push({ method, params });
        assert.equal(method, "turn/start");
        return { turn: { id: "wake-turn" } };
      },
    };
    let addressFallback = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "wake after a completed ACK",
    });
    const addressFallbackAck = parseClaudeDeliveryAck(
      `<cxmsg-ack in-reply-to="${addressFallback.jobId}" status="completed">\nAddress fallback accepted\n</cxmsg-ack>`,
    );
    addressFallback = await handleClaudeDeliveryAck(
      "coordinator",
      {
        fromSession: null,
        fromAddress: peer.address,
        fromName: peer.name,
        messageId: "32345678-1234-1234-1234-123456789abc",
      },
      addressFallbackAck,
      {
        deliverMessage: async (_target, parsed) => {
          wakes.push(parsed);
          return deliverPeerMessage(
            wakeClient,
            { id: "source-thread", status: { type: "idle" }, turns: [] },
            {
              from: "claude-reviewer",
              message: parsed.body,
              messageId: parsed.messageId,
            },
          );
        },
      },
    );
    assert.equal(addressFallback.status, "completed");
    assert.equal(addressFallback.wake.status, "delivered");
    assert.equal(wakes.length, 1);
    assert.equal(wakes[0].messageId, addressFallback.jobId);
    assert.match(wakes[0].body, /Address fallback accepted/);
    assert.equal(wakeCalls.length, 1);
    assert.equal(wakeCalls[0].method, "turn/start");
    assert.equal(
      wakeCalls[0].params.clientUserMessageId,
      addressFallback.jobId,
    );
    assert.equal(wakeCalls[0].params.approvalPolicy, "never");

    await handleClaudeDeliveryAck(
      "coordinator",
      {
        fromSession: null,
        fromAddress: peer.address,
        fromName: peer.name,
        messageId: "42345678-1234-1234-1234-123456789abc",
      },
      addressFallbackAck,
      {
        deliverMessage: async () => assert.fail("duplicate ACK must not wake twice"),
      },
    );
    assert.equal(wakes.length, 1);

    const wrongSession = "77654321-4321-4321-4321-cba987654321";
    const sessionMismatch = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "reject a mismatched session",
    });
    const sessionMismatchAck = parseClaudeDeliveryAck(
      `<cxmsg-ack in-reply-to="${sessionMismatch.jobId}" status="completed">\nReject me\n</cxmsg-ack>`,
    );
    await assert.rejects(
      recordClaudeDeliveryAck(
        { fromSession: wrongSession, fromAddress: peer.address },
        sessionMismatchAck,
      ),
      (error) => {
        assert.equal(error.code, "EACKSOURCE");
        assert.doesNotMatch(error.message, new RegExp(wrongSession));
        assert.doesNotMatch(error.message, new RegExp(peer.sessionId));
        assert.doesNotMatch(error.message, new RegExp(peer.address));
        assert.match(error.message, /actual-session=present:[0-9a-f]{10}/);
        return true;
      },
    );
    const rejectedSession = readJob(sessionMismatch.jobId);
    assert.equal(rejectedSession.status, "ack_rejected");
    assert.equal(rejectedSession.ack.code, "source_mismatch");
    assert.equal(rejectedSession.delivery.errorCode, "source_mismatch");
    assert.equal(
      (await refreshClaudeDelivery(rejectedSession)).status,
      "ack_rejected",
    );

    const addressMismatch = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "reject a mismatched address",
    });
    const addressMismatchAck = parseClaudeDeliveryAck(
      `<cxmsg-ack in-reply-to="${addressMismatch.jobId}" status="completed">\nReject me too\n</cxmsg-ack>`,
    );
    await assert.rejects(
      recordClaudeDeliveryAck(
        {
          fromSession: peer.sessionId,
          fromAddress: "uds:/tmp/cc-socks/54321.sock",
        },
        addressMismatchAck,
      ),
      /ACK source mismatch/,
    );
    assert.equal(readJob(addressMismatch.jobId).status, "ack_rejected");

    const vanishedPeer = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "record target resolution failure",
    });
    const failedResolution = await sendClaudeDeliveryJob(
      { socketPath: "/tmp/cc-socks/99999.sock" },
      sourceRecord,
      vanishedPeer,
      { peers: async () => [], send: async () => assert.fail("must not send") },
    );
    assert.equal(failedResolution.status, "transport_error");
    assert.match(failedResolution.error, /not reachable/);
    assert.equal(readJob(vanishedPeer.jobId).status, "transport_error");

    let timedOut = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "check ACK timeout",
    });
    timedOut = await updateJob(timedOut, {
      status: "transport_delivered",
      delivery: {
        ...timedOut.delivery,
        ackDeadlineAt: new Date(Date.now() - 1_000).toISOString(),
      },
    });
    assert.equal((await refreshClaudeDelivery(timedOut)).status, "ack_timeout");
  } finally {
    await fs.rm(stateDir, { recursive: true, force: true });
    delete process.env.CXMSG_STATE_DIR;
  }
});
