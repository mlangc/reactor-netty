package reactor.netty;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerResponse;
import wiremock.org.eclipse.jetty.http.HttpStatus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.System.out;
import static org.assertj.core.api.Assertions.assertThat;

public class BugReproducerTest {
	static final int DELAY_MS = 200;
	static final int NUM_ATTEMPTS = 20;
	static final int SO_TIMEOUT_MS = 5000;

	static List<TestCase> testCases() {
		List<TestCase> testCases = new ArrayList<>();
		for (boolean zeroContentLength : Arrays.asList(true, false)) {
			TestCase testCase = new TestCase();
			testCase.chunkSize = 8 * 1024;
			testCase.numChunks = 512;
			testCase.zeroContentLength = zeroContentLength;
			testCases.add(testCase);

			testCase = new TestCase();
			testCase.chunkSize = 8 * 1024 * 512;
			testCase.numChunks = 1;
			testCase.zeroContentLength = zeroContentLength;
			testCases.add(testCase);
		}

		return testCases;
	}

	@ParameterizedTest
	@MethodSource("testCases")
	void pipelinedWritesShouldNotStall(TestCase testCase) throws IOException {
		DisposableServer server = HttpServer.create()
				.host("localhost")
				.route(routes -> {
					routes.post("/initial", (req, resp) ->
							req.receive().then(
									Mono.fromRunnable(() -> out.println("Received initial request"))
											.then(Mono.defer(() -> {
												HttpServerResponse respHandle = resp.status(HttpResponseStatus.OK);
												if (testCase.zeroContentLength) {
													respHandle = respHandle.header(HttpHeaderNames.CONTENT_LENGTH, "0");
												}

												return respHandle.sendHeaders()
														.then()
														.then(Mono.fromRunnable(() -> out.println("Responded to delay request")))
														.then(Mono.delay(Duration.ofMillis(DELAY_MS)))
														.then(Mono.fromRunnable(() -> out.println("Delay request processing done")));
											}))));

					routes.post("/data", (req, resp) ->
							Mono.fromRunnable(() -> out.println("Received data request"))
									.then(req.receive()
											.doOnNext(buffer -> { })
											.then(resp.status(HttpResponseStatus.OK).send())
											.then(Mono.fromRunnable(() -> out.println("Data request processed")))));
				}).bindNow();

		try {
			final byte[] chunk = new byte[testCase.chunkSize];
			Arrays.fill(chunk, (byte) 'x');

			for (int i = 0; i < NUM_ATTEMPTS; i++) {
				try (Socket socket = new Socket("localhost", server.port())) {
					socket.setSoTimeout(SO_TIMEOUT_MS);

					OutputStream socketOut = socket.getOutputStream();
					BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));

					socketOut.write("POST /initial HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n"
							.getBytes(StandardCharsets.US_ASCII));
					socketOut.flush();

					assertThat(socketReader.readLine()).startsWith("HTTP/1.1 200");

					if (testCase.zeroContentLength) {
						assertThat(socketReader.readLine()).isEqualTo("content-length: 0");
					}
					else {
						assertThat(socketReader.readLine()).isEqualTo("transfer-encoding: chunked");
					}

					CompletableFuture<Void> secondWrite = CompletableFuture.runAsync(() -> {
						try {
							socketOut.write(("POST /data HTTP/1.1\r\nHost: localhost\r\nContent-Length: "
							                 + (testCase.chunkSize * testCase.numChunks) + "\r\n\r\n")
									.getBytes(StandardCharsets.US_ASCII));
							socketOut.flush();

							for (int j = 0; j < testCase.numChunks; j++) {
								socketOut.write(chunk);
								socketOut.flush();
							}

							out.println("Writing " + testCase.numChunks + " chunks succeeded");
						}
						catch (IOException e) {
							out.println("Write failed with " + e);
							throw new UncheckedIOException(e);
						}
					});

					try {
						secondWrite.get(5, TimeUnit.SECONDS);
						List<String> lines = new ArrayList<>();

						String line;
						while ((line = socketReader.readLine()) != null) {
							lines.add(line);
							if (line.endsWith("close") || line.startsWith("HTTP/1.1 200")) {
								break;
							}
						}

						assertThat(lines)
								.as("lines=%s", lines)
								.isNotEmpty()
								.last().satisfies(l -> assertThat(l).contains("200"));
					}
					catch (Exception e) {
						assertThat(e).isNotInstanceOf(TimeoutException.class)
								.hasRootCauseInstanceOf(IOException.class);
					}
				}
			}
		}
		finally {
			server.disposeNow(Duration.ofSeconds(5));
		}
	}

	static class TestCase {
		int chunkSize;
		int numChunks;
		boolean zeroContentLength;

		@Override
		public String toString() {
			return "TestCase{" +
			       ", chunkSize=" + chunkSize +
			       ", numChunks=" + numChunks +
			       ", zeroContentLength=" + zeroContentLength +
			       '}';
		}
	}
}
