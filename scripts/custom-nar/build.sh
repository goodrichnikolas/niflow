#!/usr/bin/env bash
# Build a REAL NiFi NAR containing one custom processor, with Docker only.
#
# Why: niflow's rulebooks are harvested from stock Apache containers, so a
# custom type — which is what work actually runs — was never exercised. This
# builds one: compile against the nifi-api jar taken out of the NiFi image
# itself (so the version always matches), package the classes into a jar, and
# wrap that jar in a NAR the way Maven's nifi-nar-maven-plugin does.
#
# A NAR is a zip with:
#   META-INF/MANIFEST.MF                       <- Nar-Id / Nar-Group / Nar-Version
#   META-INF/bundled-dependencies/<name>.jar   <- the classes, plus their
#                                                 META-INF/services registration
#
# Output: <outdir>/niflow-test-nar-<version>.nar. Mount it into a NiFi's
# ./extensions directory — 1.9+ hot-loads what it finds there.
#
#   ./scripts/custom-nar/build.sh [outdir] [nifi-version]
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
outdir="${1:-$here/../../.nifi-nars}"
nifi_version="${2:-1.24.0}"
nar_version="1.0.0"
nar_name="niflow-test-nar-${nar_version}.nar"

mkdir -p "$outdir"
if [ -f "$outdir/$nar_name" ]; then
    echo "$outdir/$nar_name already exists — delete it to rebuild."
    exit 0
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

echo "Taking nifi-api out of apache/nifi:${nifi_version}..."
cid="$(docker create "apache/nifi:${nifi_version}")"
docker cp "$cid:/opt/nifi/nifi-current/lib/nifi-api-${nifi_version}.jar" "$work/nifi-api.jar"
docker rm "$cid" >/dev/null

cp -r "$here/src" "$work/src"
mkdir -p "$work/classes/META-INF/services" "$work/nar/META-INF/bundled-dependencies"
cp "$here/services.txt" "$work/classes/META-INF/services/org.apache.nifi.processor.Processor"

cat > "$work/manifest.txt" <<MANIFEST
Nar-Id: niflow-test-nar
Nar-Group: com.niflow.test
Nar-Version: ${nar_version}
MANIFEST

echo "Compiling and packaging..."
# The NiFi image ships a JRE, not a JDK, so the compile happens in a JDK image.
# --release 11 keeps the bytecode loadable by NiFi 1.x (Java 11) as well as 2.x.
# --user: the build writes into $work, and root-owned files there would
# survive the trap that cleans it up.
docker run --rm --user "$(id -u):$(id -g)" -v "$work:/w" -w /w eclipse-temurin:21-jdk bash -c '
    set -e
    javac --release 11 -cp nifi-api.jar -d classes $(find src -name "*.java")
    (cd classes && jar --create --file ../niflow-test-processors.jar .)
    cp niflow-test-processors.jar nar/META-INF/bundled-dependencies/
    (cd nar && jar --create --file /w/out.nar --manifest /w/manifest.txt .)
'

cp "$work/out.nar" "$outdir/$nar_name"
echo "Built $outdir/$nar_name"
