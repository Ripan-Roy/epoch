#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
java_source="$repo_root/sdk/java"
consumer_source="$repo_root/tests/release/artifacts/java-consumer"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/epoch-java-artifacts.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

project_dir="$work_dir/project"
consumer_dir="$work_dir/consumer"
local_repository="$work_dir/maven-repository"
mkdir -p "$project_dir" "$consumer_dir"

cp -R "$java_source/.mvn" "$java_source/config" "$java_source/src" "$project_dir/"
cp "$java_source/mvnw" "$java_source/pom.xml" "$project_dir/"
cp -R "$consumer_source/." "$consumer_dir/"
cp -R "$java_source/.mvn" "$consumer_dir/"
cp "$java_source/mvnw" "$consumer_dir/"

central_configuration=$(
  sed -n \
    '/<artifactId>central-publishing-maven-plugin<\/artifactId>/,/<\/plugin>/p' \
    "$project_dir/pom.xml"
)
if ! grep -Fq '<skipPublishing>true</skipPublishing>' <<<"$central_configuration"; then
  echo "the central-bundle profile must hardcode skipPublishing=true" >&2
  exit 1
fi
if ! grep -Fq '<autoPublish>false</autoPublish>' <<<"$central_configuration"; then
  echo "the central-bundle profile must hardcode autoPublish=false" >&2
  exit 1
fi

(
  cd "$project_dir"
  ./mvnw \
    --batch-mode \
    --no-transfer-progress \
    -Dmaven.repo.local="$local_repository" \
    -Pcentral-bundle \
    clean install
)

version=$(
  sed -n 's/^[[:space:]]*<version>\([^<]*\)<\/version>.*/\1/p' "$project_dir/pom.xml" \
    | head -n 1
)
if [[ -z "$version" ]]; then
  echo "could not determine the Java SDK version" >&2
  exit 1
fi

artifact_prefix="$project_dir/target/epoch-sdk-$version"
main_jar="$artifact_prefix.jar"
sources_jar="$artifact_prefix-sources.jar"
javadoc_jar="$artifact_prefix-javadoc.jar"

for artifact in "$main_jar" "$sources_jar" "$javadoc_jar"; do
  if [[ ! -s "$artifact" ]]; then
    echo "missing Java release artifact: $artifact" >&2
    exit 1
  fi
done

jar tf "$main_jar" >"$work_dir/main-jar.txt"
jar tf "$sources_jar" >"$work_dir/sources-jar.txt"
jar tf "$javadoc_jar" >"$work_dir/javadoc-jar.txt"

grep -Fxq 'io/epoch/sdk/EpochClient.class' "$work_dir/main-jar.txt"
grep -Fxq 'io/epoch/sdk/EventEnvelope.class' "$work_dir/main-jar.txt"
grep -Fxq 'io/epoch/sdk/EpochClient.java' "$work_dir/sources-jar.txt"
grep -Fxq 'io/epoch/sdk/EventEnvelope.java' "$work_dir/sources-jar.txt"
grep -Fxq 'index.html' "$work_dir/javadoc-jar.txt"

unzip -p "$main_jar" META-INF/MANIFEST.MF \
  | tr -d '\r' \
  >"$work_dir/manifest.txt"
grep -Fxq 'Automatic-Module-Name: io.epoch.sdk' "$work_dir/manifest.txt"

coordinate_path="io/epoch/epoch-sdk/$version"
installed_prefix="$local_repository/$coordinate_path/epoch-sdk-$version"
for suffix in \
  ".pom" \
  ".jar" \
  "-sources.jar" \
  "-javadoc.jar"; do
  if [[ ! -s "$installed_prefix$suffix" ]]; then
    echo "the isolated Maven repository is missing epoch-sdk-$version$suffix" >&2
    exit 1
  fi
done

if find "$local_repository/$coordinate_path" -type f -name '*.asc' -print -quit | grep -q .; then
  echo "unexpected signature found: this gate must not manufacture signing evidence" >&2
  exit 1
fi

(
  cd "$consumer_dir"
  ./mvnw \
    --batch-mode \
    --no-transfer-progress \
    -Dmaven.repo.local="$local_repository" \
    -Depoch.sdk.version="$version" \
    verify \
    exec:java \
    -Dexec.mainClass=io.epoch.releaseprobe.ArtifactConsumer
)

printf '%s\n' \
  "Java artifact shape verified for io.epoch:epoch-sdk:$version." \
  "The artifacts are local and unsigned; Central upload and publication remain disabled."
