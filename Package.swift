// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "transmit-swift",
    platforms: [.iOS(.v15), .macOS(.v12)],
    products: [.library(name: "Transmit", targets: ["Transmit"])],
    targets: [
        .target(name: "Transmit"),
        .testTarget(name: "TransmitTests", dependencies: ["Transmit"]),
    ]
)
