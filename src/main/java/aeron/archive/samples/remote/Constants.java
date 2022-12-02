package aeron.archive.samples.remote;

public interface Constants {
  int STREAM_ID = 10;
  int RECORDED_STREAM_ID = 10;
  int REPLAY_STREAM_ID = 20;
  String ARCHIVE_URI = "aeron:udp?endpoint=localhost:2000";
  String AERON_UDP_ENDPOINT = "aeron:udp?endpoint=";
  String ARCHIVE_HOST = "localhost";
  String CONTROL_PORT = "8088";
}
