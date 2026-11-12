{ lib, buildGo127Module }:

# Core-only package. Drivers are independent executables installed alongside the
# gateway or supplied through PATH / LLM_GATEWAY_DRIVERS_DIR.
buildGo127Module {
  pname = "llm-gateway";
  version = "0.1.0";

  # The core package does not need independent drivers or local build caches.
  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [ ./go.mod ./go.sum ./cmd ./driverproto ./internal ];
  };
  vendorHash = "sha256-qVoj03LNLbdoCUAOydK7oEHsuZ1BZ6Z2jwYB3gPOfrw=";
  subPackages = [ "cmd/llm-gateway" ];

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    go test ./driverproto ./internal/model ./internal/policy ./internal/session ./internal/structured ./internal/telemetry
    runHook postCheck
  '';

  meta = {
    description = "OpenAI-compatible gateway core with independently installed drivers";
    homepage = "https://github.com/mytecor/llm-gateway";
    license = lib.licenses.mit;
    mainProgram = "llm-gateway";
    platforms = lib.platforms.unix;
  };
}
