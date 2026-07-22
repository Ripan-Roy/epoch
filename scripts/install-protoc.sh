#!/bin/sh

set -eu

PROTOC_VERSION="35.1"
LINUX_X86_64_SHA256="6930ebf62bd4ea607b98fff052596c6ee564b9835b4ce172c75a3f53ae9d91b7"
LINUX_AARCH64_SHA256="01bf9d08808c7f96678b63f4bd8efa559bb4f83d5a7a270d5edaf507f9d5d9cf"

usage() {
    echo "usage: $0 /absolute/destination" >&2
}

fail() {
    echo "install-protoc: $*" >&2
    exit 1
}

if [ "$#" -ne 1 ]; then
    usage
    exit 64
fi

destination=$1
case "$destination" in
    /*) ;;
    *) fail "destination must be an absolute path" ;;
esac

destination_name=$(basename -- "$destination")
case "$destination_name" in
    "" | "." | "..") fail "destination must name a new directory" ;;
esac

destination_parent=$(dirname -- "$destination")
[ -d "$destination_parent" ] || fail "destination parent does not exist: $destination_parent"
[ ! -e "$destination" ] || fail "destination already exists: $destination"

[ "$(uname -s)" = "Linux" ] || fail "only Linux is supported"

case "$(uname -m)" in
    x86_64 | amd64)
        archive_arch="x86_64"
        expected_sha256=$LINUX_X86_64_SHA256
        ;;
    aarch64 | arm64)
        archive_arch="aarch_64"
        expected_sha256=$LINUX_AARCH64_SHA256
        ;;
    *) fail "unsupported Linux architecture: $(uname -m)" ;;
esac

for command_name in curl sha256sum unzip; do
    command -v "$command_name" >/dev/null 2>&1 || fail "required command is missing: $command_name"
done

archive_name="protoc-${PROTOC_VERSION}-linux-${archive_arch}.zip"
archive_url="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${archive_name}"

umask 022
work_directory=$(mktemp -d "${destination_parent}/.epoch-protoc.XXXXXX") ||
    fail "could not create a temporary directory under $destination_parent"

cleanup() {
    rm -rf -- "$work_directory"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

archive_path="${work_directory}/${archive_name}"
staging_path="${work_directory}/staging"
mkdir "$staging_path"

curl --fail --location --proto '=https' --tlsv1.2 --retry 3 \
    --output "$archive_path" "$archive_url"

printf '%s  %s\n' "$expected_sha256" "$archive_path" | sha256sum --check --status ||
    fail "SHA-256 verification failed for $archive_name"

unzip -q "$archive_path" -d "$staging_path"
[ -x "$staging_path/bin/protoc" ] || fail "archive does not contain executable bin/protoc"
[ -d "$staging_path/include" ] || fail "archive does not contain the Protobuf include directory"

actual_version=$("$staging_path/bin/protoc" --version)
[ "$actual_version" = "libprotoc ${PROTOC_VERSION}" ] ||
    fail "archive reported '$actual_version'; expected 'libprotoc ${PROTOC_VERSION}'"

mv -T -- "$staging_path" "$destination"
echo "installed libprotoc ${PROTOC_VERSION} at ${destination}/bin/protoc"
