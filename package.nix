{ lib, buildGo127Module }:

# Core-only package. Drivers are independent executables installed alongside the
# gateway or supplied through PATH / LLM_GATEWAY_DRIVERS_DIR.
buildGo127Module {
  pname = "llm-gateway";
  version = "0.1.0";

  src = ./.;
  vendorHash = null;
  subPackages = [ "cmd/llm-gateway" ];

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    go test ./driverproto ./internal/model ./internal/policy ./internal/session ./internal/telemetry
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
