package aeron.archive.samples.remote;

import static aeron.archive.samples.remote.Constants.AERON_UDP_ENDPOINT;
import static aeron.archive.samples.remote.Constants.ARCHIVE_HOST;
import static aeron.archive.samples.remote.Constants.CONTROL_PORT;
import static aeron.archive.samples.remote.Constants.RECORDED_STREAM_ID;
import static aeron.archive.samples.remote.Constants.REPLAY_STREAM_ID;
import static aeron.archive.samples.remote.Constants.ARCHIVE_URI;
import static org.agrona.CloseHelper.quietClose;

import baseline.BondDecoder;
import baseline.MessageHeaderDecoder;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

public class Server implements Agent {
  private static final MessageHeaderDecoder HEADER_DECODER = new MessageHeaderDecoder();
  private static final BondDecoder MSG_DECODER = new BondDecoder();
  private final String host = "localhost";
  private int itemCount = 0;
  private Aeron aeron;
  private MediaDriver mediaDriver;
  private State state;
  private IdleStrategy idleStrategy;
  private AeronArchive archive;
  private AeronArchive.AsyncConnect asyncConnect;
  private Subscription replayDestinationSubs;

  public Server() {
    this.idleStrategy = new SleepingMillisIdleStrategy(250);

    System.out.println(" --- Launching Media Driver --- ");
    this.mediaDriver =
        MediaDriver.launch(
            new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnShutdown(true)
                .aeronDirectoryName("/dev/shm/lom-server")
                .sharedIdleStrategy(new SleepingMillisIdleStrategy()));

    System.out.println(
        " --- Connecting Aeron; Media Driver directory ::: "
            + mediaDriver.aeronDirectoryName()
            + " --- ");

    this.aeron =
        Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .idleStrategy(new SleepingMillisIdleStrategy()));

    this.state = State.AERON_READY;
  }

  public static void main(String[] args) {
    var serverRunner =
        new AgentRunner(
            new BusySpinIdleStrategy(),
            Throwable::printStackTrace,
            null,
            new Server());

    AgentRunner.startOnThread(serverRunner);

    new ShutdownSignalBarrier().await();

    System.out.println("################### SERVER stop ###################");
    quietClose(serverRunner);
  }

  @Override
  public void onStart() {
    System.out.println("#################### SERVER start ####################");
    Agent.super.onStart();
  }

  @Override
  public int doWork() {
    switch (state) {
      case AERON_READY -> connectToArchive();
      case POLLING_SUBSCRIPTION -> replayDestinationSubs.poll(this::handler, 1);
      default -> System.err.println(" !!! Unknown state ::: " + state);
    }

    return 0;
  }

  @Override
  public void onClose() {
    Agent.super.onClose();
    System.out.println("############### SERVER shutting down ##############");
    quietClose(replayDestinationSubs);
    quietClose(archive);
    quietClose(aeron);
    quietClose(mediaDriver);
  }

  private void connectToArchive() {
    if (asyncConnect == null) {
      System.out.println(" --- Connecting Aeron Archive --- ");
      asyncConnect =
          AeronArchive.asyncConnect(
              new AeronArchive.Context()
                  .controlRequestChannel(AERON_UDP_ENDPOINT + ARCHIVE_HOST + ":" + CONTROL_PORT)
                  .controlResponseChannel(AERON_UDP_ENDPOINT + host + ":0")
                  .aeron(aeron));
    } else {
      // if the archive hasn't been set yet, poll it after idling 250ms
      if (null == archive) {
        System.out.println(" --- Awaiting Aeron Archive --- ");
        idleStrategy.idle();
        try {
          archive = asyncConnect.poll();
        } catch (TimeoutException e) {
          System.out.println(" --- timeout --- ");
          asyncConnect = null;
        }
      } else {
        System.out.println(" --- Finding remote Recording --- ");

        final var recordingId = getRecordingId(ARCHIVE_URI, RECORDED_STREAM_ID);
        if (recordingId != Long.MIN_VALUE) {
          var localReplayChannelEphemeral = AERON_UDP_ENDPOINT + host + ":0";
          replayDestinationSubs =
              aeron.addSubscription(localReplayChannelEphemeral, REPLAY_STREAM_ID);
          var actualReplayChannel = replayDestinationSubs.tryResolveChannelEndpointPort();

          System.out.println(" --- ActualReplayChannel ::: " + actualReplayChannel + " --- ");

          // replay from the Archive Recording start
          long replaySession =
              archive.startReplay(
                  recordingId, 0L, Long.MAX_VALUE, actualReplayChannel, REPLAY_STREAM_ID);

          System.out.println(
              " --- Ready to poll subscription, replaying to "
                  + actualReplayChannel
                  + ", image is ::: "
                  + (int) replaySession
                  + " --- ");

          state = State.POLLING_SUBSCRIPTION;
        } else {
          // await the remote host being ready, idle 250ms
          idleStrategy.idle();
        }
      }
    }
  }

  private long getRecordingId(final String remoteRecordedChannel, final int remoteRecordedStream) {
    final var lastRecordingId = new MutableLong();
    final RecordingDescriptorConsumer consumer =
        (controlSessionId,
            correlationId,
            recordingId,
            startTimestamp,
            stopTimestamp,
            startPosition,
            stopPosition,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity) -> lastRecordingId.set(recordingId);

    final var fromRecordingId = 0L;
    final var recordCount = 100;

    final int foundCount =
        archive.listRecordingsForUri(
            fromRecordingId, recordCount, remoteRecordedChannel, remoteRecordedStream, consumer);

    if (0 == foundCount) {
      return Long.MIN_VALUE;
    }

    return lastRecordingId.get();
  }

  private void handler(DirectBuffer buffer, int offset, int length, Header header) {
    System.out.println("[" + Thread.currentThread().getName() + "] ::: message # : " + ++itemCount);
    try {
      decode(buffer, offset, itemCount);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String roleName() {
    return "RECEIVER";
  }

  void decode(DirectBuffer buffer, int offset, int msgNum) throws Exception {
    MSG_DECODER.wrapAndApplyHeader(buffer, offset, HEADER_DECODER);

    System.err.println(
        "//////////////////////// MSG TOP [" + msgNum + "] ////////////////////////");
    final StringBuilder sb = new StringBuilder();
    sb.append("bond.serialNumber=").append(MSG_DECODER.serialNumber());
    sb.append("\nbond.expiration=").append(MSG_DECODER.expiration());
    sb.append("\nbond.available=").append(MSG_DECODER.available());
    sb.append("\nbond.rating=").append(MSG_DECODER.rating());
    sb.append("\nbond.code=").append(MSG_DECODER.code());

    sb.append("\nbond.someNumbers=");
    for (int i = 0, size = BondDecoder.someNumbersLength(); i < size; i++) {
      sb.append(MSG_DECODER.someNumbers(i)).append(", ");
    }

    final byte[] temp = new byte[128];
    final UnsafeBuffer tempBuffer = new UnsafeBuffer(temp);
    final int tempBuffLength = MSG_DECODER.getDesc(tempBuffer, 0, tempBuffer.capacity());
    sb.append("\nbond.desc=")
        .append(new String(temp, 0, tempBuffLength, BondDecoder.descCharacterEncoding()));

    System.err.println(sb);
    System.err.println(
        "//////////////////////// MSG END [" + msgNum + "] ////////////////////////");
  }

  public enum State {
    AERON_READY,
    POLLING_SUBSCRIPTION
  }
}
