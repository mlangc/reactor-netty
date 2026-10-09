package reactor.netty;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.System.out;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

/**
 * Bisected on 1.3.x between v1.3.6 and upstream/1.3.x (ffa911cff):
 * last passing commit: 3acbf95c4 "Refine header handling during redirects";
 * first failing commit: e530b7bd3 "Add back-pressure on pipelined HTTP requests".
 * <p>
 * Older releases: the test compiles and passes on all of v1.3.0-M1 to v1.3.0-M7, v1.3.0-RC1,
 * v1.3.0 and v1.3.1 to v1.3.6.
 */
public class BugReproducerTest {
	static final int DELAY_MS = 200;
	static final int NUM_ATTEMPTS = 20;
	static final int SO_TIMEOUT_MS = 5000;

	@ParameterizedTest
	@CsvSource({"100_000, true", "100_000, false", "1_000_000, true", "1_000_000, false"})
	void pipelinedWritesShouldNotStall(int payloadBytes, boolean zeroContentLength) throws IOException, ExecutionException, InterruptedException {
		DisposableServer server = HttpServer.create()
				.host("localhost")
				.route(routes -> {
					routes.post("/initial", (req, resp) -> {
						if (zeroContentLength) {
							resp.header(HttpHeaderNames.CONTENT_LENGTH, "0");
						}

						return req.receive()
								.then(Mono.fromRunnable(() -> out.println("Received initial request")))
								.then(resp.sendHeaders().then())
								.then(Mono.fromRunnable(() -> out.println("Responded to delay request")))
								.then(Mono.delay(Duration.ofMillis(DELAY_MS)))
								.then(Mono.fromRunnable(() -> out.println("Delay request processing done")));
					});

					routes.post("/data", (req, resp) ->
							req.receive()
									.then(resp.status(HttpResponseStatus.OK).send())
									.doFirst(() -> out.println("Received data request"))
									.doOnSuccess(ignore -> out.println("Data request processed")));
				}).bindNow();

		try {
			final byte[] payload = new byte[payloadBytes];
			Arrays.fill(payload, (byte) 'x');

			for (int i = 0; i < NUM_ATTEMPTS; i++) {
				try (Socket socket = new Socket("localhost", server.port())) {
					socket.setSoTimeout(SO_TIMEOUT_MS);

					OutputStream socketOut = socket.getOutputStream();
					BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));

					socketOut.write("POST /initial HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n"
							.getBytes(StandardCharsets.US_ASCII));
					socketOut.flush();

					assertThat(socketReader.readLine()).startsWith("HTTP/1.1 200");

					assertThat(socketReader.readLine()).isEqualTo(
							zeroContentLength ? "content-length: 0" : "transfer-encoding: chunked");

					CompletableFuture<Void> secondWrite = CompletableFuture.runAsync(() -> {
						try {
							socketOut.write(("POST /data HTTP/1.1\r\nHost: localhost\r\nContent-Length: "
							                 + payloadBytes + "\r\n\r\n")
									.getBytes(StandardCharsets.US_ASCII));
							socketOut.write(payload);
							socketOut.flush();

							out.println("Writing " + payloadBytes + " bytes succeeded");
						}
						catch (IOException e) {
							out.println("Write failed with " + e);
							throw new UncheckedIOException(e);
						}
					});

					try {
						secondWrite.get(SO_TIMEOUT_MS, TimeUnit.MILLISECONDS);
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
					} catch (TimeoutException e) {
						fail("Write stalled", e);
					} catch (SocketTimeoutException e) {
						fail("Read stalled", e);
					}
				}
			}
		}
		finally {
			server.disposeNow(Duration.ofSeconds(5));
		}
	}
}
