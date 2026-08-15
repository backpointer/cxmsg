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
      recordClaudeDeliveryReply,
      recordClaudeNativeDeliveryReceipt,
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
      logicalMessageId: "17654321-4321-4321-4321-cba987654321",
      replyToMessageId: "27654321-4321-4321-4321-cba987654321",
    });
    assert.deepEqual(job.correlation, {
      kind: "peer-reply",
      logicalMessageId: "17654321-4321-4321-4321-cba987654321",
      replyToMessageId: "27654321-4321-4321-4321-cba987654321",
    });
    assert.equal(job.jobId, job.correlation.logicalMessageId);
    await assert.rejects(
      createClaudeDeliveryJob({
        from: "coordinator",
        sourceRecord,
        peer,
        message: "invalid unbounded completion",
        completionTimeoutSeconds: 86_401,
      }),
      /completion timeout must be an integer from 1 to 86400 seconds/,
    );
    const duplicateReply = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "review the change",
      maxAttempts: 3,
      logicalMessageId: job.correlation.logicalMessageId,
      replyToMessageId: job.correlation.replyToMessageId,
    });
    assert.equal(duplicateReply.deduplicated, true);
    assert.equal(duplicateReply.jobId, job.jobId);
    await assert.rejects(
      createClaudeDeliveryJob({
        from: "coordinator",
        sourceRecord,
        peer,
        message: "changed reply content",
        logicalMessageId: job.correlation.logicalMessageId,
        replyToMessageId: job.correlation.replyToMessageId,
      }),
      /idempotency conflict/,
    );
    await assert.rejects(
      createClaudeDeliveryJob({
        from: "coordinator",
        sourceRecord,
        peer,
        message: "review the change",
        completionTimeoutSeconds: 901,
        logicalMessageId: job.correlation.logicalMessageId,
        replyToMessageId: job.correlation.replyToMessageId,
      }),
      /idempotency conflict/,
    );
    const legacyLogicalMessageId = "a7654321-4321-4321-4321-cba987654321";
    const legacyReplyToMessageId = "b7654321-4321-4321-4321-cba987654321";
    let legacyTimeoutJob = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "legacy timeout default",
      logicalMessageId: legacyLogicalMessageId,
      replyToMessageId: legacyReplyToMessageId,
    });
    legacyTimeoutJob = await updateJob(legacyTimeoutJob, {
      completionTimeoutSeconds: undefined,
    });
    const legacyTimeoutDuplicate = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "legacy timeout default",
      logicalMessageId: legacyLogicalMessageId,
      replyToMessageId: legacyReplyToMessageId,
    });
    assert.equal(legacyTimeoutDuplicate.deduplicated, true);
    const concurrentCorrelation = {
      logicalMessageId: "37654321-4321-4321-4321-cba987654321",
      replyToMessageId: "47654321-4321-4321-4321-cba987654321",
    };
    const concurrent = await Promise.all([
      createClaudeDeliveryJob({
        from: "coordinator",
        sourceRecord,
        peer,
        message: "one correlated reply",
        ...concurrentCorrelation,
      }),
      createClaudeDeliveryJob({
        from: "coordinator",
        sourceRecord,
        peer,
        message: "one correlated reply",
        ...concurrentCorrelation,
      }),
    ]);
    assert.equal(concurrent.filter((candidate) => candidate.deduplicated).length, 1);
    assert.equal(concurrent[0].jobId, concurrent[1].jobId);
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
    assert.match(frames[0].message.content, /status=accepted/);
    assert.match(frames[0].message.content, /status=completed/);

    job = await recordClaudeNativeDeliveryReceipt({
      messageId: frames[0].msg_id,
      status: "held",
    });
    assert.equal(job.status, "transport_delivered");
    assert.equal(job.delivery.nativeReceipts.length, 1);
    assert.equal(job.delivery.nativeReceipts[0].status, "held");
    job = await recordClaudeNativeDeliveryReceipt({
      messageId: frames[0].msg_id,
      status: "delivered",
    });
    assert.equal(job.status, "transport_delivered");
    assert.equal(job.delivery.nativeReceipts.length, 1);
    assert.equal(job.delivery.nativeReceipts[0].status, "delivered");
    const duplicateNativeReceipt = await recordClaudeNativeDeliveryReceipt({
      messageId: frames[0].msg_id,
      status: "delivered",
    });
    assert.equal(duplicateNativeReceipt.nativeReceiptTransitioned, false);
    await assert.rejects(
      recordClaudeNativeDeliveryReceipt({
        messageId: frames[0].msg_id,
        status: "expired",
      }),
      (error) => error.code === "ENATIVERECEIPTCONFLICT",
    );
    assert.equal(
      readJob(job.jobId).delivery.nativeReceipts[0].status,
      "delivered",
    );
    const unmatchedEvents = [];
    assert.equal(
      await recordClaudeNativeDeliveryReceipt(
        {
          messageId: "62345678-1234-1234-1234-123456789abc",
          status: "delivered",
        },
        { log: async (event) => unmatchedEvents.push(event) },
      ),
      null,
    );
    assert.equal(unmatchedEvents.length, 1);
    assert.equal(unmatchedEvents[0].phase, "native-receipt-unmatched");
    assert.equal(unmatchedEvents[0].errorCode, "unknown_message");

    const ambiguousMessageId = "92345678-1234-1234-1234-123456789abc";
    const ambiguousOne = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "first ambiguous receipt fixture",
    });
    const ambiguousTwo = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "second ambiguous receipt fixture",
    });
    await updateJob(ambiguousOne, {
      delivery: {
        ...ambiguousOne.delivery,
        messageIds: [ambiguousMessageId],
      },
    });
    await updateJob(ambiguousTwo, {
      delivery: {
        ...ambiguousTwo.delivery,
        messageIds: [ambiguousMessageId],
      },
    });
    await assert.rejects(
      recordClaudeNativeDeliveryReceipt({
        messageId: ambiguousMessageId,
        status: "delivered",
      }),
      (error) => error.code === "ENATIVERECEIPTAMBIGUOUS",
    );

    const correlatedReply = await recordClaudeDeliveryReply(
      {
        fromSession: peer.sessionId,
        fromAddress: peer.address,
        messageId: "72345678-1234-1234-1234-123456789abc",
      },
      job.jobId,
    );
    assert.equal(correlatedReply.status, "transport_delivered");
    assert.equal(correlatedReply.ack, null);
    assert.equal(correlatedReply.replyEvidence.status, "correlated");
    assert.equal(correlatedReply.replyEvidence.late, false);
    const duplicateReplyEvidence = await recordClaudeDeliveryReply(
      {
        fromSession: peer.sessionId,
        fromAddress: peer.address,
        messageId: correlatedReply.replyEvidence.messageId,
      },
      job.jobId,
    );
    assert.equal(duplicateReplyEvidence.replyTransitioned, false);

    const concurrentReply = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "serialize two structured replies",
    });
    const concurrentReplyResults = await Promise.allSettled([
      recordClaudeDeliveryReply(
        {
          fromSession: peer.sessionId,
          fromAddress: peer.address,
          messageId: "a2345678-1234-1234-1234-123456789abc",
        },
        concurrentReply.jobId,
      ),
      recordClaudeDeliveryReply(
        {
          fromSession: peer.sessionId,
          fromAddress: peer.address,
          messageId: "b2345678-1234-1234-1234-123456789abc",
        },
        concurrentReply.jobId,
      ),
    ]);
    assert.equal(
      concurrentReplyResults.filter((result) => result.status === "fulfilled").length,
      1,
    );
    const rejectedConcurrentReply = concurrentReplyResults.find(
      (result) => result.status === "rejected",
    );
    assert.equal(rejectedConcurrentReply.reason.code, "EREPLYCONFLICT");
    assert.ok(
      [
        "a2345678-1234-1234-1234-123456789abc",
        "b2345678-1234-1234-1234-123456789abc",
      ].includes(readJob(concurrentReply.jobId).replyEvidence.messageId),
    );

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

    const acceptedAck = parseClaudeDeliveryAck(
      `<cxmsg-ack in-reply-to="${job.jobId}" status="accepted">\nQueued for review\n</cxmsg-ack>`,
    );
    job = await recordClaudeDeliveryAck(
      {
        fromSession: peer.sessionId,
        fromAddress: peer.address,
      },
      acceptedAck,
    );
    assert.equal(job.status, "acknowledged");
    assert.ok(job.delivery.acceptedAt);
    assert.ok(job.delivery.completionDeadlineAt);
    assert.equal(job.delivery.nextAttemptAt, null);
    const acceptedDeadline = job.delivery.completionDeadlineAt;
    const duplicateAccepted = await recordClaudeDeliveryAck(
      {
        fromSession: peer.sessionId,
        fromAddress: peer.address,
      },
      acceptedAck,
    );
    assert.equal(duplicateAccepted.ackTransitioned, false);
    assert.equal(duplicateAccepted.delivery.completionDeadlineAt, acceptedDeadline);
    await assert.rejects(
      recordClaudeDeliveryAck(
        {
          fromSession: peer.sessionId,
          fromAddress: peer.address,
        },
        retryAck,
      ),
      (error) => error.code === "EACKTRANSITION",
    );
    assert.equal(readJob(job.jobId).status, "acknowledged");

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

    let completionTimedOut = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "check completion timeout",
      completionTimeoutSeconds: 1,
    });
    completionTimedOut = await sendClaudeDeliveryJob(
      { socketPath: "/tmp/cc-socks/99999.sock" },
      sourceRecord,
      completionTimedOut,
      { peers: async () => [peer], send: async () => {} },
    );
    const completionAccepted = parseClaudeDeliveryAck(
      `<cxmsg-ack in-reply-to="${completionTimedOut.jobId}" status="accepted">\nStarted\n</cxmsg-ack>`,
    );
    completionTimedOut = await recordClaudeDeliveryAck(
      { fromSession: peer.sessionId, fromAddress: peer.address },
      completionAccepted,
    );
    completionTimedOut = await updateJob(completionTimedOut, {
      delivery: {
        ...completionTimedOut.delivery,
        completionDeadlineAt: new Date(Date.now() - 1_000).toISOString(),
      },
    });
    completionTimedOut = await refreshClaudeDelivery(completionTimedOut);
    assert.equal(completionTimedOut.status, "completion_timeout");
    assert.match(completionTimedOut.error, /did not complete/);
    const lateCompletion = parseClaudeDeliveryAck(
      `<cxmsg-ack in-reply-to="${completionTimedOut.jobId}" status="completed">\nLate completion\n</cxmsg-ack>`,
    );
    completionTimedOut = await recordClaudeDeliveryAck(
      { fromSession: peer.sessionId, fromAddress: peer.address },
      lateCompletion,
    );
    assert.equal(completionTimedOut.status, "completed");
    assert.equal(completionTimedOut.ack.late, true);
    assert.ok(completionTimedOut.delivery.completionDeadlineAt);

    const rotatedPeer = {
      ...peer,
      name: "reviewer-renamed",
      address: "uds:/tmp/cc-socks/54321.sock",
      socketPath: "/tmp/cc-socks/54321.sock",
    };
    let rotated = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "follow the stable Node to its current endpoint",
    });
    const selectedSockets = [];
    rotated = await sendClaudeDeliveryJob(
      { socketPath: "/tmp/cc-socks/99999.sock" },
      sourceRecord,
      rotated,
      {
        peers: async () => [rotatedPeer],
        send: async (socketPath) => selectedSockets.push(socketPath),
      },
    );
    assert.deepEqual(selectedSockets, [rotatedPeer.socketPath]);
    assert.equal(rotated.claudeTarget.sessionId, peer.sessionId);
    assert.equal(rotated.claudeTarget.name, rotatedPeer.name);
    assert.equal(rotated.claudeTarget.address, rotatedPeer.address);

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

    const blockedWake = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "preserve an externally owned writer failure",
    });
    const blockedWakeAck = parseClaudeDeliveryAck(
      `<cxmsg-ack in-reply-to="${blockedWake.jobId}" status="completed">\nStored result\n</cxmsg-ack>`,
    );
    await assert.rejects(
      handleClaudeDeliveryAck(
        "coordinator",
        {
          fromSession: peer.sessionId,
          fromAddress: peer.address,
          fromName: peer.name,
          messageId: "52345678-1234-1234-1234-123456789abc",
        },
        blockedWakeAck,
        {
          deliverMessage: async () => {
            const error = new Error("external rollout writer is not managed");
            error.code = "EEXTERNALWRITERUNVERIFIED";
            throw error;
          },
        },
      ),
      (error) => error.code === "EEXTERNALWRITERUNVERIFIED",
    );
    const storedBlockedWake = readJob(blockedWake.jobId);
    assert.equal(storedBlockedWake.status, "completed");
    assert.equal(storedBlockedWake.result, "Stored result");
    assert.equal(storedBlockedWake.wake.status, "failed");
    assert.equal(
      storedBlockedWake.wake.errorCode,
      "EEXTERNALWRITERUNVERIFIED",
    );

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

    const wrongReplySource = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "reject a mismatched structured reply",
    });
    await assert.rejects(
      recordClaudeDeliveryReply(
        {
          fromSession: "57654321-4321-4321-4321-cba987654321",
          fromAddress: peer.address,
          messageId: "82345678-1234-1234-1234-123456789abc",
        },
        wrongReplySource.jobId,
      ),
      (error) => error.code === "EREPLYSOURCE",
    );
    assert.equal(readJob(wrongReplySource.jobId).replyEvidence, undefined);

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
    timedOut = await refreshClaudeDelivery(timedOut);
    assert.equal(timedOut.status, "ack_timeout");
    await assert.rejects(
      recordClaudeDeliveryAck(
        {
          fromSession: "67654321-4321-4321-4321-cba987654321",
          fromAddress: peer.address,
        },
        parseClaudeDeliveryAck(
          `<cxmsg-ack in-reply-to="${timedOut.jobId}" status="completed">\nWrong late source\n</cxmsg-ack>`,
        ),
      ),
      (error) => error.code === "EACKSOURCE",
    );
    assert.equal(readJob(timedOut.jobId).status, "ack_timeout");
    const lateAck = parseClaudeDeliveryAck(
      `<cxmsg-ack in-reply-to="${timedOut.jobId}" status="completed">\nLate result\n</cxmsg-ack>`,
    );
    timedOut = await recordClaudeDeliveryAck(
      { fromSession: peer.sessionId, fromAddress: peer.address },
      lateAck,
    );
    assert.equal(timedOut.status, "completed");
    assert.equal(timedOut.ack.late, true);

    let raceSafe = await createClaudeDeliveryJob({
      from: "coordinator",
      sourceRecord,
      peer,
      message: "do not overwrite a terminal ACK with a stale refresh",
    });
    raceSafe = await sendClaudeDeliveryJob(
      { socketPath: "/tmp/cc-socks/99999.sock" },
      sourceRecord,
      raceSafe,
      { peers: async () => [peer], send: async () => {} },
    );
    raceSafe = await updateJob(raceSafe, {
      delivery: {
        ...raceSafe.delivery,
        ackDeadlineAt: new Date(Date.now() - 1_000).toISOString(),
      },
    });
    const staleRaceCandidate = raceSafe;
    const raceCompletion = parseClaudeDeliveryAck(
      `<cxmsg-ack in-reply-to="${raceSafe.jobId}" status="completed">\nWon the race\n</cxmsg-ack>`,
    );
    raceSafe = await recordClaudeDeliveryAck(
      { fromSession: peer.sessionId, fromAddress: peer.address },
      raceCompletion,
    );
    assert.equal(raceSafe.status, "completed");
    raceSafe = await refreshClaudeDelivery(staleRaceCandidate);
    assert.equal(raceSafe.status, "completed");

    const eventLog = await fs.readFile(path.join(stateDir, "events.jsonl"), "utf8");
    const events = eventLog.trim().split("\n").map((line) => JSON.parse(line));
    assert.ok(events.some((event) => event.phase === "transport-attempt"));
    assert.ok(
      events.some(
        (event) => event.phase === "ack-persisted" && event.late === true,
      ),
    );
    assert.doesNotMatch(eventLog, /review the change|Late result|cc-socks/);
  } finally {
    await fs.rm(stateDir, { recursive: true, force: true });
    delete process.env.CXMSG_STATE_DIR;
  }
});
