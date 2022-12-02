package aeron.archive.samples;

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
import java.io.File;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

public class ArchiveHelloWorldExample {
  public static final String CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8010";
  public static final String CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
  public static final String REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";
  public static final String IPC_CHANNEL = "aeron:ipc";
  private static final int STREAM_ID = 33;
  private static final int sendCount = 10_000;
  private static final IdleStrategy idleStrategy = new SleepingIdleStrategy();
  private static final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
  private static int recordingNumber = 0;

  public static void main(final String[] args) {
    try (ArchivingMediaDriver ignore = createArchivingMD();
        Aeron aeron = Aeron.connect();
        AeronArchive aeronArchive =
            AeronArchive.connect(
                new AeronArchive.Context()
                    .aeron(aeron)
                    .controlRequestChannel(CONTROL_REQUEST_CHANNEL)
                    .controlResponseChannel(CONTROL_RESPONSE_CHANNEL)
                    .idleStrategy(new SleepingMillisIdleStrategy()))) {

      System.out.println(" --- starting recording --- ");
      aeronArchive.startRecording(IPC_CHANNEL, STREAM_ID, SourceLocation.LOCAL);

      System.out.println(" --- creating publication --- ");
      try (Publication publication = aeron.addExclusivePublication(IPC_CHANNEL, STREAM_ID)) {
        idleWaiting(!publication.isConnected());

        publishTestData(publication);

        var stopPosition = publication.position();
        var countersReader = aeron.countersReader();
        var counterId =
            RecordingPos.findCounterIdBySession(countersReader, publication.sessionId());

        idleWaiting(countersReader.getCounterValue(counterId) < stopPosition);

        System.err.println("Recording is completed & Stop position is ::: " + stopPosition);
      }

    } catch (final Exception ex) {
      ex.printStackTrace();
    }
  }

  private static void publishTestData(Publication publication) {
    for (var i = 0; i <= sendCount; i++) {
      buffer.putInt(0, i);
      idleWaiting(publication.offer(buffer, 0, Integer.BYTES) < 0);
    }
  }

  private static void idleWaiting(boolean condition) {
    while (condition) {
      idleStrategy.idle();
    }
  }

  private static ArchivingMediaDriver createArchivingMD() {
    final MediaDriver.Context driverContext =
        new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .errorHandler(Throwable::printStackTrace)
            .spiesSimulateConnection(true)
            .dirDeleteOnStart(true);

    final Archive.Context archiveContext =
        new Archive.Context()
            .deleteArchiveOnStart(true)
            .archiveDir(new File("archive-lom-ipc"))
            .controlChannel(CONTROL_REQUEST_CHANNEL)
            .replicationChannel(REPLICATION_CHANNEL)
            .threadingMode(ArchiveThreadingMode.SHARED);

    System.err.println(
        "############### Creating basic archive at "
            + archiveContext.archiveDir()
            + " folder ###############");

    return ArchivingMediaDriver.launch(driverContext, archiveContext);
  }
}
