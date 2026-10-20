{ lib, buildGo127Module }:

buildGo127Module {
  pname = "llm-gateway";
  version = "0.1.0";

  src = ./.;
  vendorHash = "sha256-Ct6NZQjVGMsOBIUVgUFOZm2yHMDLwPEskpE3KOFfYuM=";

  subPackages = [ "cmd/llm-gateway" ];
  checkFlags = [ "./..." ];

  doCheck = true;

  meta = {
    description = "OpenAI-compatible gateway with API, ACP and browser executors";
    homepage = "https://github.com/mytecor/llm-gateway";
    license = lib.licenses.mit;
    mainProgram = "llm-gateway";
    platforms = lib.platforms.unix;
  };
}
