{
  description = "backup-controller: fill a PersistentVolumeClaim from a restic repository through VolSync's mover, leaving no ZFS clone behind";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          # Everything every task calls through `nix develop -c`. Nothing is
          # fetched at shell entry; the flake is the whole toolchain.
          packages = with pkgs; [
            go_1_26
            gopls
            golangci-lint
            kubernetes-controller-tools # controller-gen
            kubectl
            kustomize
            kubernetes-helm # helm
            yq-go
            actionlint
            hadolint
            setup-envtest
            git
          ];
        };
      });
    };
}
