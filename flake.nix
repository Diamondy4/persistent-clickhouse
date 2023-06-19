{
  description = "clickhouse-driver";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs";
    flake-parts.url = "github:hercules-ci/flake-parts";
    haskell-flake.url = "github:srid/haskell-flake";
    clickhouse-driver = {
      url = "github:GetShopTV/clickhouse-driver";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = inputs @ {
    self,
    flake-parts,
    nixpkgs,
    ...
  }:
    flake-parts.lib.mkFlake {inherit inputs;} {
      systems = nixpkgs.lib.systems.flakeExposed;
      imports = [
        inputs.haskell-flake.flakeModule
      ];
      perSystem = {self', ...}: {
        haskellProjects.default = {
          imports = [
            inputs.clickhouse-driver.haskellFlakeProjectModules.output
          ];
        };
        packages.default = self'.packages.persistent-clickhouse;
      };
    };
}
