package aeron.archive.samples.remote;

import static aeron.archive.samples.remote.Client.State.ARCHIVE_READY;
import static aeron.archive.samples.remote.Constants.AERON_UDP_ENDPOINT;
import static aeron.archive.samples.remote.Constants.ARCHIVE_HOST;
import static aeron.archive.samples.remote.Constants.ARCHIVE_URI;
import static aeron.archive.samples.remote.Constants.CONTROL_PORT;
import static aeron.archive.samples.remote.Constants.STREAM_ID;
import static org.agrona.CloseHelper.quietClose;

import baseline.BondEncoder;
import baseline.BooleanType;
import baseline.MessageHeaderEncoder;
import baseline.Rating;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

public class Client implements Agent {
  private static final EpochClock CLOCK = SystemEpochClock.INSTANCE;
  private static final BondEncoder MSG_ENCODER = new BondEncoder();
  private static final MessageHeaderEncoder HEADER_ENCODER = new MessageHeaderEncoder();
  private static final byte[] CODE;
  private static final UnsafeBuffer DESC;

  static {
    try {
      CODE = "BOND07".getBytes(BondEncoder.codeCharacterEncoding());
      DESC = new UnsafeBuffer("USA GOV BOND [expiration=2025, discount=10%]".getBytes(
          BondEncoder.codeCharacterEncoding()));
    } catch (final UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }
  }

  private final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(4096));
  private final Aeron aeron;
  private State state;
  private Publication publication;
  private ArchivingMediaDriver archivingMediaDriver;
  private IdleStrategy idleStrategy;
  private AeronArchive archive;
  private long nextAppend = Long.MIN_VALUE;
  private int lastSeq = 0;

  public Client() {
    this.archivingMediaDriver = launchMediaDriver();
    this.aeron = launchAeron(archivingMediaDriver);
    this.state = State.AERON_READY;
    this.idleStrategy = new SleepingIdleStrategy();
  }

  public static void main(String[] args) {
    AgentRunner clientRunner = new AgentRunner(new BusySpinIdleStrategy(),
        Throwable::printStackTrace, null, new Client());
    AgentRunner.startOnThread(clientRunner);

    new ShutdownSignalBarrier().await();
    System.out.println("################### CLIENT stop ###################");
    quietClose(clientRunner);
  }

  @Override
  public void onStart() {
    System.out.println("################### CLIENT start ###################");
    Agent.super.onStart();
  }

  @Override
  public void onClose()
  {
    Agent.super.onClose();
    System.out.println("############### CLIENT shutting down ##############");
    quietClose(publication);
    quietClose(aeron);
    quietClose(archivingMediaDriver);
  }

  @Override
  public int doWork()
  {
    switch (state)
    {
      case AERON_READY -> createArchiveAndRecord();
      case ARCHIVE_READY -> appendData();
      default -> System.err.println(" !!! Unknown state {} " + state);
    }

    return 0;
  }

  private void createArchiveAndRecord()
  {
    System.out.println(" --- Creating Archive --- ");
    archive = AeronArchive.connect(new AeronArchive.Context()
        .aeron(aeron)
        .controlRequestChannel(AERON_UDP_ENDPOINT + ARCHIVE_HOST + ":" + CONTROL_PORT)
        .controlResponseChannel(AERON_UDP_ENDPOINT + ARCHIVE_HOST + ":0")
        .idleStrategy(new SleepingMillisIdleStrategy()));

    System.out.println(" --- Creating Publication --- ");
    publication = aeron.addExclusivePublication(ARCHIVE_URI, STREAM_ID);

    System.out.println(" --- Starting Recording --- ");
    archive.startRecording(ARCHIVE_URI, STREAM_ID, SourceLocation.LOCAL);

    System.out.println(" --- Waiting for Recording to start for session ::: " + publication.sessionId());
    var counters = aeron.countersReader();
    int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());

    while (CountersReader.NULL_COUNTER_ID == counterId)
    {
      idleStrategy.idle();
      counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
    }

    final long recordingId = RecordingPos.getRecordingId(counters, counterId);
    System.out.println("Archive Recording started; recording id is ::: " +  recordingId);

    this.state = ARCHIVE_READY;
  }

  private void appendData()
  {
    if (CLOCK.time() >= nextAppend)
    {
      lastSeq += 1;
      publishTestMessage(publication, lastSeq);
      nextAppend = CLOCK.time() + 2000;
      System.out.println(" --- Appended ::: " + lastSeq + " --- ");
    }
  }

  private void publishTestMessage(Publication publication, int idx) {
    idleWaiting(publication.offer(buffer, 0, encode(buffer, idx)) < 0);
  }

  private ArchivingMediaDriver launchMediaDriver()
  {
    System.out.println(" --- Launching ArchivingMediaDriver --- ");

    final var archiveContext = new Archive.Context()
        .deleteArchiveOnStart(true)
        .errorHandler(this::errorHandler)
        .controlChannel(AERON_UDP_ENDPOINT + ARCHIVE_HOST + ":" + CONTROL_PORT)
        .idleStrategySupplier(SleepingMillisIdleStrategy::new)
        .replicationChannel(AERON_UDP_ENDPOINT + ARCHIVE_HOST + ":" + CONTROL_PORT + 1)
        .threadingMode(ArchiveThreadingMode.SHARED);

    final var mediaDriverContext = new MediaDriver.Context()
        .spiesSimulateConnection(true)
        .errorHandler(this::errorHandler)
        .threadingMode(ThreadingMode.SHARED)
        .sharedIdleStrategy(new SleepingMillisIdleStrategy())
        .aeronDirectoryName("/dev/shm/lom-client")
        .dirDeleteOnStart(true);

    return ArchivingMediaDriver.launch(mediaDriverContext, archiveContext);
  }

  private Aeron launchAeron(ArchivingMediaDriver archivingMediaDriver)
  {
    System.out.println(" --- Launching Aeron --- ");
    return Aeron.connect(new Aeron.Context()
        .aeronDirectoryName(archivingMediaDriver.mediaDriver().aeronDirectoryName())
        .errorHandler(this::errorHandler)
        .idleStrategy(new SleepingMillisIdleStrategy()));
  }

  int encode(UnsafeBuffer buffer, int idx) {
    MSG_ENCODER.wrapAndApplyHeader(buffer, 0, HEADER_ENCODER)
        .serialNumber(1230 + idx)
        .expiration(2025 + idx)
        .available(BooleanType.T)
        .rating(Rating.B)
        .putCode(CODE, 0)
        .putSomeNumbers(idx * 10, idx * 20, idx * 30,
            idx * 40)
        .putDesc(DESC, 0, DESC.capacity());

    return MessageHeaderEncoder.ENCODED_LENGTH + MSG_ENCODER.encodedLength();
  }

  private void idleWaiting(boolean condition) {
    while (condition) {
      idleStrategy.idle();
    }
  }

  @Override
  public String roleName() {
    return "SENDER";
  }

  private void errorHandler(Throwable throwable)
  {
    System.err.println(" !!! Unexpected failure ::: " + throwable.getMessage());
  }

  enum State {
    AERON_READY,
    ARCHIVE_READY
  }
}
