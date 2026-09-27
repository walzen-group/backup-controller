{
  description = "backup-controller: fill a PersistentVolumeClaim from a restic repository through VolSync's mover, leaving no ZFS clone behind";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      linux = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
      forLinux = f: nixpkgs.lib.genAttrs linux (system: f nixpkgs.legacyPackages.${system});

      # versions.json is the one list of pinned versions. The tests read it
      # too, to check that each recorded fixture came from these tools.
      versions = builtins.fromJSON (builtins.readFile ./versions.json);
      testTools = pkgs: import ./nix/test-tools.nix { inherit pkgs versions; };

      # Everything every task calls through `nix develop -c`. Nothing is
      # fetched at shell entry; the flake is the whole toolchain.
      basePackages = pkgs: with pkgs; [
        go_1_26
        gopls
        golangci-lint
        kubernetes-controller-tools # controller-gen
        kubectl
        kustomize
        kubernetes-helm # helm
        yq-go
        jq
        actionlint
        hadolint
        git
        python3 # runs the asd-ste100 skill's ste-lint.py on comments and docs
      ];
    in
    {
      # The pinned test tools, one output each, so a single one can be built
      # or run on its own (nix build .#rustfs). The control-plane binaries
      # exist only for Linux, which is where the envtest and fixture shells
      # run.
      packages = forLinux (pkgs: removeAttrs (testTools pkgs) [ "restic" "postgresql" "bubblewrap" "etcd" ]);

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = basePackages pkgs;
        };
      } // nixpkgs.lib.optionalAttrs (builtins.elem pkgs.stdenv.hostPlatform.system linux) (
        let tools = testTools pkgs; in {
          # The shell of the tagged envtest suites: a real kube-apiserver and
          # etcd at production's Kubernetes version, from the store.
          # KUBEBUILDER_ASSETS points envtest at them, so envtest downloads
          # nothing.
          envtest = pkgs.mkShell {
            packages = basePackages pkgs;
            KUBEBUILDER_ASSETS = "${tools.envtest-assets}/bin";
          };

          # The shell the fixture recorders run in: envtest's assets plus
          # every real program a recorded fixture comes from.
          fixtures = pkgs.mkShell {
            packages = basePackages pkgs ++ (with tools; [
              restic
              restic-mover
              rustfs
              barman
              postgresql
              bubblewrap
              kube-controller-manager
              kube-scheduler
            ]) ++ [ pkgs.coreutils pkgs.util-linux pkgs.curl ];
            KUBEBUILDER_ASSETS = "${tools.envtest-assets}/bin";
          };
        }
      ));
    };
}
