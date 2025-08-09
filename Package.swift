<<<<<<< HEAD
// swift-tools-version:6.1
=======
// swift-tools-version:5.8
>>>>>>> 4f39868e27b697e89ae2e76ff0ffe4318c55cd2c
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

// For pre-update hook support:
//  - Uncomment lines containing `SQLITE_ENABLE_PREUPDATE_HOOK`
//
// For session support:
//  - Uncomment lines containing `SQLITE_ENABLE_PREUPDATE_HOOK`
//  - Uncomment lines containing `SQLITE_ENABLE_SESSION`
<<<<<<< HEAD
//
// For RBU support ("Resumable Bulk Update")
//  - Uncomment lines containing `SQLITE_ENABLE_RBU`
=======
>>>>>>> 4f39868e27b697e89ae2e76ff0ffe4318c55cd2c

let package = Package(
	name: "Pipeline",
	platforms: [
<<<<<<< HEAD
        .macOS(.v15),
        .iOS(.v17),
	],
	products: [
=======
		.macOS(.v10_15),
		.iOS(.v13),
	],
	products: [
		// Products define the executables and libraries a package produces, and make them visible to other packages.
>>>>>>> 4f39868e27b697e89ae2e76ff0ffe4318c55cd2c
		.library(
			name: "Pipeline",
			targets: ["Pipeline"]),
	],
	dependencies: [
		// Dependencies declare other packages that this package depends on.
		// .package(url: /* package url */, from: "1.0.0"),
<<<<<<< HEAD
//		.package(url: "https://github.com/wildthink/CSQLite", from: "3.47.0")
//        .package(url: "https://github.com/wildthink/CSQLite", branch: "main")
=======
		.package(url: "https://github.com/sbooth/CSQLite", from: "3.44.0")
>>>>>>> 4f39868e27b697e89ae2e76ff0ffe4318c55cd2c
	],
	targets: [
		// Targets are the basic building blocks of a package. A target can define a module or a test suite.
		// Targets can depend on other targets in this package, and on products in packages this package depends on.
		.target(
			name: "CPipeline",
<<<<<<< HEAD
			dependencies: []),
		.target(
			name: "Pipeline",
			dependencies: [
                "CPipeline",
//                "CSQLite",
            ],
			cSettings: [
			],
			swiftSettings: [
=======
			dependencies: ["CSQLite"]),
		.target(
			name: "Pipeline",
			dependencies: ["CSQLite", "CPipeline"],
			cSettings: [
//				.define("SQLITE_ENABLE_PREUPDATE_HOOK", to: "1"),
//				.define("SQLITE_ENABLE_SESSION", to: "1"),
			],
			swiftSettings: [
//				.define("SQLITE_ENABLE_PREUPDATE_HOOK"),
//				.define("SQLITE_ENABLE_SESSION"),
>>>>>>> 4f39868e27b697e89ae2e76ff0ffe4318c55cd2c
			]),
		.testTarget(
			name: "PipelineTests",
			dependencies: ["Pipeline"],
<<<<<<< HEAD
            cSettings: [
            ],
			swiftSettings: [
=======
			swiftSettings: [
//				.define("SQLITE_ENABLE_PREUPDATE_HOOK"),
//				.define("SQLITE_ENABLE_SESSION"),
>>>>>>> 4f39868e27b697e89ae2e76ff0ffe4318c55cd2c
			]),
	],
	cLanguageStandard: .gnu11
)
