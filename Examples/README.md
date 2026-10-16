# AdonisJS → SwiftUI example

A runnable backend and iOS app that use the local `transmit-swift` package. The backend publishes on `chats/general` every five seconds and accepts manual messages through `POST /broadcast`. The app confirms its subscription with `awaitSubscribed`, publishes a greeting through that endpoint, and lists received chat messages, connection status and the number of server instances it has seen.

Requirements: Node.js 24, Xcode 16 or later, and an installed iOS Simulator runtime. The app's deployment target is iOS 15. Run the commands below from the repository root. The Xcode project is included; no project generator is needed.

## Start the backend

```sh
npm ci --prefix Examples/Backend --no-audit --no-fund
npm start --prefix Examples/Backend
```

It binds to `http://127.0.0.1:3333` and prints `READY`, `SUBSCRIBED` and `PUBLISHED` events. Keep this terminal running. In a second terminal:

```sh
curl http://127.0.0.1:3333/health
curl -X POST http://127.0.0.1:3333/broadcast \
  -H 'Content-Type: application/json' \
  -d '{"text":"Hello from Adonis"}'
```

Messages contain `id`, `text`, `source`, `serverInstance`, `seq` and `sentAt`. Every backend start gets a new `serverInstance`; the sequence starts over. The `/broadcast` endpoint requires a nonempty text string of at most 200 characters.

This is a loopback-only development example without authentication. Its app permits local networking through App Transport Security, and the backend uses a development-only application key. The iOS Simulator can reach the Mac's loopback address; these instructions are for Simulator use.

## Run the app in Xcode

```sh
open Examples/iOS/TransmitExample.xcodeproj
```

Choose the **TransmitExample** scheme, select an iPhone Simulator, and run. The project resolves the `Transmit` product from `../..`, the repository root, so changes to the library are used immediately.

After `awaitSubscribed("chats/general")` succeeds, the app publishes "Hello from the SwiftUI client" and receives its own greeting; timer messages follow every five seconds. Send a manual broadcast to see it at the top of the list. The app deduplicates increasing `seq` values per server instance, retains the latest 100 messages and logs each received message using the `io.elcubo.TransmitExample` subsystem. Its SwiftUI task owns the subscription and explicitly unsubscribes and closes the client when the task ends; `onDisappear` also performs that cleanup. The app configures a 90-second SSE read timeout and displays terminal authentication failures.

## Build, launch and inspect from the terminal

List available devices, then set `SIMULATOR_ID` to the UDID of the iPhone you want to use:

```sh
xcrun simctl list devices available
export SIMULATOR_ID="your-iPhone-simulator-UDID"
```

If that device is shut down, boot it; skip this command if it is already booted:

```sh
xcrun simctl boot "$SIMULATOR_ID"
```

Build and launch:

```sh
xcrun simctl bootstatus "$SIMULATOR_ID" -b
xcodebuild -project Examples/iOS/TransmitExample.xcodeproj \
  -scheme TransmitExample -destination "id=$SIMULATOR_ID" \
  -derivedDataPath /tmp/transmit-example-build CODE_SIGNING_ALLOWED=NO build
xcrun simctl install "$SIMULATOR_ID" \
  /tmp/transmit-example-build/Build/Products/Debug-iphonesimulator/TransmitExample.app
xcrun simctl launch "$SIMULATOR_ID" io.elcubo.TransmitExample
xcrun simctl spawn "$SIMULATOR_ID" log stream --style compact --level info \
  --predicate 'subsystem == "io.elcubo.TransmitExample"'
```

`PUBLISHED after subscription confirmation` confirms the greeting POST was sent after subscription acknowledgement. The log stream stays open; use another terminal for broadcasts and screenshots. `RECEIVED` lines include the server instance, sequence, text and stable client UID. Use an explicit device UDID when multiple simulators are booted.

For another port, start the backend with `PORT=3334 npm start --prefix Examples/Backend`, then launch the app with:

```sh
SIMCTL_CHILD_TRANSMIT_BASE_URL=http://127.0.0.1:3334 \
  xcrun simctl launch --terminate-running-process "$SIMULATOR_ID" io.elcubo.TransmitExample
```

You can also set `TRANSMIT_BASE_URL` in the Xcode scheme's Run environment variables.

## Verify reconnection

1. Keep the app running. Publish a message and confirm both its `RECEIVED` log and its card in the app.
2. Capture the first screenshot:

   ```sh
   xcrun simctl io "$SIMULATOR_ID" screenshot /tmp/transmit-example.png
   ```

3. Stop the backend with Control-C. The app changes to **Reconnecting**. Start the backend again using the same command and port; do not relaunch the app.
4. Wait for the backend's `SUBSCRIBED` line, then publish another message. The app automatically restores the channel and receives both new timer messages and the manual broadcast. The client UID stays the same, while the message's server instance changes and **SERVER SESSIONS** increases to 2.
5. Capture the second screenshot:

   ```sh
   xcrun simctl io "$SIMULATOR_ID" screenshot /tmp/transmit-example-reconnect.png
   ```

Reconnect delays grow from 1 to 30 seconds, so a longer outage may take up to 30 seconds to recover. Messages published while disconnected are not replayed. Screenshots and build output stay outside the repository.

Stop the backend with Control-C when finished. To stop the example app:

```sh
xcrun simctl terminate "$SIMULATOR_ID" io.elcubo.TransmitExample
```

## Library checks

The example is outside the Swift package's source and test targets. The existing GitHub Actions workflow still checks the library; it does not run this Simulator demonstration. To reproduce those library checks locally:

```sh
swift build
swift test
npm ci --prefix Tests/IntegrationServer --no-audit --no-fund
TRANSMIT_INTEGRATION=1 swift test --enable-code-coverage
python3 scripts/check-coverage.py
```
