# The tools the tests and the fixture recorders run, each pinned to the version
# versions.json names. Release binaries are fetched by version and hash, the
# way the infra repository pins the RustFS rc client; the rest come from the
# locked nixpkgs, and an assertion stops the shell when nixpkgs has moved to
# another version than versions.json records. Nothing here is downloaded at
# test time: every binary is a store path by the time a test starts.
{ pkgs, versions }:
let
  inherit (pkgs) lib;
  system = pkgs.stdenv.hostPlatform.system;
  v = versions.components;

  # nixpkgs must carry the version versions.json names, or the fixtures
  # recorded with it would no longer match the tool a person runs.
  fromNixpkgs =
    name: package:
    assert lib.assertMsg (package.version == v.${name}.version)
      "nixpkgs ${name} is ${package.version}, versions.json says ${v.${name}.version}; update versions.json and run make fixtures";
    package;

  # One Kubernetes release binary from dl.k8s.io. Only Linux builds exist.
  kubeBinary =
    name:
    let
      arch =
        {
          "x86_64-linux" = "amd64";
          "aarch64-linux" = "arm64";
        }
        .${system};
    in
    pkgs.stdenvNoCC.mkDerivation {
      pname = name;
      version = v.kubernetes.version;
      src = pkgs.fetchurl {
        url = "https://dl.k8s.io/v${v.kubernetes.version}/bin/linux/${arch}/${name}";
        hash = v.kubernetes.hashes.${name}.${system};
      };
      dontUnpack = true;
      installPhase = ''
        install -Dm755 $src $out/bin/${name}
      '';
      meta.platforms = [
        "x86_64-linux"
        "aarch64-linux"
      ];
    };

  kube-apiserver = kubeBinary "kube-apiserver";
  kube-controller-manager = kubeBinary "kube-controller-manager";
  kube-scheduler = kubeBinary "kube-scheduler";

  etcd = fromNixpkgs "etcd" pkgs.etcd_3_6;

  # The directory envtest reads through KUBEBUILDER_ASSETS: kube-apiserver and
  # etcd side by side, with kubectl for the envtest helpers that shell out to
  # it.
  envtest-assets = pkgs.runCommand "envtest-assets-${v.kubernetes.version}" { } ''
    mkdir -p $out/bin
    ln -s ${kube-apiserver}/bin/kube-apiserver $out/bin/kube-apiserver
    ln -s ${etcd}/bin/etcd $out/bin/etcd
    ln -s ${pkgs.kubectl}/bin/kubectl $out/bin/kubectl
  '';

  # restic as the VolSync mover image ships it. It is installed as
  # restic-<version> so it sits beside nixpkgs restic on one PATH.
  restic-mover =
    let
      asset =
        {
          "x86_64-linux" = "linux_amd64";
          "aarch64-linux" = "linux_arm64";
          "x86_64-darwin" = "darwin_amd64";
          "aarch64-darwin" = "darwin_arm64";
        }
        .${system};
      version = v.restic-mover.version;
    in
    pkgs.stdenvNoCC.mkDerivation {
      pname = "restic-mover";
      inherit version;
      src = pkgs.fetchurl {
        url = "https://github.com/restic/restic/releases/download/v${version}/restic_${version}_${asset}.bz2";
        hash = v.restic-mover.hashes.${system};
      };
      nativeBuildInputs = [ pkgs.bzip2 ];
      dontUnpack = true;
      installPhase = ''
        mkdir -p $out/bin
        bzip2 -dc $src > $out/bin/restic-${version}
        chmod 755 $out/bin/restic-${version}
      '';
    };

  # The RustFS server, the S3 implementation walzen prod stores backups on.
  # It has no nixpkgs package; the release zip holds one static binary.
  rustfs =
    let
      asset =
        {
          "x86_64-linux" = "linux-x86_64-musl";
          "aarch64-linux" = "linux-aarch64-musl";
          "aarch64-darwin" = "macos-aarch64";
        }
        .${system};
      version = v.rustfs.version;
    in
    pkgs.stdenvNoCC.mkDerivation {
      pname = "rustfs";
      inherit version;
      src = pkgs.fetchurl {
        url = "https://github.com/rustfs/rustfs/releases/download/${version}/rustfs-${asset}-v${version}.zip";
        hash = v.rustfs.hashes.${system};
      };
      nativeBuildInputs = [ pkgs.unzip ];
      sourceRoot = ".";
      dontConfigure = true;
      dontBuild = true;
      installPhase = ''
        install -Dm755 rustfs $out/bin/rustfs
      '';
      meta = {
        description = "S3-compatible object storage server";
        homepage = "https://github.com/rustfs/rustfs";
        license = lib.licenses.asl20;
        mainProgram = "rustfs";
        platforms = [
          "x86_64-linux"
          "aarch64-linux"
          "aarch64-darwin"
        ];
      };
    };

  # barman at the version the Barman Cloud plugin's sidecar pins. nixpkgs
  # carries an older release, so only its source moves; the build and the
  # dependencies stay nixpkgs'. Its own test suite is not run here. 3.20.0
  # moved the package under src/ and builds with uv_build, so the build
  # system and the path nixpkgs substitutes change with it. nixpkgs' one patch
  # (unwrap-subprocess.patch) changes how the barman server spawns itself,
  # which the barman-cloud tools never do, and it no longer applies, so it is
  # dropped.
  barman = pkgs.barman.overridePythonAttrs (old: {
    version = v.barman.version;
    src = pkgs.fetchFromGitHub {
      owner = "EnterpriseDB";
      repo = "barman";
      tag = "release/${v.barman.version}";
      hash = v.barman.hash;
    };
    patches = [ ];
    postPatch = ''
      substituteInPlace src/barman/encryption.py \
        --replace-fail '"file"' '"${lib.getExe pkgs.file}"'
    '';
    build-system = [ pkgs.python3Packages.uv-build ];
    dependencies = old.dependencies ++ [
      pkgs.python3Packages.cramjam
      pkgs.python3Packages.zstandard
      pkgs.python3Packages.lz4
    ];
    doCheck = false;
    doInstallCheck = false;
  });
in
{
  inherit
    kube-apiserver
    kube-controller-manager
    kube-scheduler
    etcd
    envtest-assets
    restic-mover
    rustfs
    barman
    ;
  restic = fromNixpkgs "restic" pkgs.restic;
  postgresql = fromNixpkgs "postgresql" pkgs.postgresql_18;
  bubblewrap = fromNixpkgs "bubblewrap" pkgs.bubblewrap;
}
