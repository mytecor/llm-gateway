{ lib, buildGo127Module }:

buildGo127Module {
  pname = "llm-gateway";
  version = "0.1.0";

  src = ./.;
  vendorHash = "sha256-B/415ItBAOXkwiVUxDzFXqkx2BAoF2Ygz84/0uTui9o=";

  subPackages = [ "cmd/llm-gateway" ];
  checkFlags = [ "./..." ];

  doCheck = true;

  meta = {
    description = "OpenAI-compatible routing proxy built on the Bifrost Go API";
    homepage = "https://github.com/mytecor/llm-gateway";
    license = lib.licenses.mit;
    mainProgram = "llm-gateway";
    platforms = lib.platforms.unix;
  };
}
