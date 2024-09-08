Contributing to InfluxDB Cluster
========================

Bug reports
---------------
Before you file an issue, please search existing issues in case it has already been filed, or perhaps even fixed. If you file an issue, please include the following.
* Full details of your operating system (or distribution) e.g. 64-bit Ubuntu 14.04.
* The version of InfluxDB Cluster you are running
* Whether you installed it using a pre-built package, or built it from source.
* A small test case, if applicable, that demonstrates the issues.

Remember the golden rule of bug reports: **The easier you make it for us to reproduce the problem, the faster it will get fixed.**
If you have never written a bug report before, or if you want to brush up on your bug reporting skills, we recommend reading [Simon Tatham's essay "How to Report Bugs Effectively."](http://www.chiark.greenend.org.uk/~sgtatham/bugs.html)

Test cases should be in the form of `curl` commands. For example:
```bash
# create database
curl -X POST http://localhost:8086/query --data-urlencode "q=CREATE DATABASE mydb"

# create retention policy
curl -X POST http://localhost:8086/query --data-urlencode "q=CREATE RETENTION POLICY myrp ON mydb DURATION 365d REPLICATION 1 DEFAULT"

# write data
curl -X POST http://localhost:8086/write?db=mydb --data-binary "cpu,region=useast,host=server_1,service=redis value=61"

# Delete a Measurement
curl -X POST http://localhost:8086/query  --data-urlencode 'db=mydb' --data-urlencode 'q=DROP MEASUREMENT cpu'

# Query the Measurement
# Bug: expected it to return no data, but data comes back.
curl -X POST http://localhost:8086/query  --data-urlencode 'db=mydb' --data-urlencode 'q=SELECT * from cpu'
```
**If you don't include a clear test case like this, your issue may not be investigated, and may even be closed**. If writing the data is too difficult, please zip up your data directory and include a link to it in your bug report.

Feature requests
---------------
We really like to receive feature requests, as it helps us prioritize our work. Please be clear about your requirements, as incomplete feature requests may simply be closed if we don't understand what you would like to see added to InfluxDB Cluster.

Contributing to the source code
---------------

InfluxDB Cluster follows standard Go project structure. This means that all your Go development are done in `$GOPATH/src`. GOPATH can be any directory under which InfluxDB Cluster and all its dependencies will be cloned. For full details on the project structure, follow along below.

You should also read our [coding guide](https://github.com/chengshiwen/influxdb-cluster/blob/master/CODING_GUIDELINES.md), to understand better how to write code for InfluxDB Cluster.

Submitting a pull request
------------
To submit a pull request you should fork the InfluxDB Cluster repository, and make your change on a feature branch of your fork. Then generate a pull request from your branch against *master* of the InfluxDB Cluster repository. Include in your pull request details of your change -- the why *and* the how -- as well as the testing your performed. Also, be sure to run the test suite with your change in place. Changes that cause tests to fail cannot be merged.

There will usually be some back and forth as we finalize the change, but once that completes it may be merged.

To assist in review for the PR, please add the following to your pull request comment:

```md
- [ ] CHANGELOG.md updated
- [ ] Rebased/mergable
- [ ] Tests pass
```

Installing Go
-------------
InfluxDB Cluster requires Go 1.21

At InfluxDB Cluster we find gvm, a Go version manager, useful for installing Go. For instructions
on how to install it see [the gvm page on github](https://github.com/moovweb/gvm).

After installing gvm you can install and set the default go version by
running the following:

    gvm install go1.21
    gvm use go1.21 --default

Revision Control Systems
-------------
Go has the ability to import remote packages via revision control systems with the `go get` command.  To ensure that you can retrieve any remote package, be sure to install the following rcs software to your system.
Currently the project only depends on `git` and `mercurial`.

* [Install Git](http://git-scm.com/book/en/Getting-Started-Installing-Git)
* [Install Mercurial](http://mercurial.selenic.com/wiki/Download)

Getting the source
------
Setup the project structure and fetch the repo like so:

```bash
    mkdir $HOME/gocodez
    export GOPATH=$HOME/gocodez
    git clone https://github.com/chengshiwen/influxdb-cluster.git
```

You can add the line `export GOPATH=$HOME/gocodez` to your bash/zsh file to be set for every shell instead of having to manually run it everytime.

Cloning a fork
-------------
If you wish to work with fork of InfluxDB Cluster, your own fork for example, you must still follow the directory structure above. But instead of cloning the main repo, instead clone your fork. Follow the steps below to work with a fork:

```bash
    export GOPATH=$HOME/gocodez
    git clone git@github.com:<username>/influxdb-cluster
```

Build and Test
-----

Make sure you have Go installed and the project structure as shown above.
To then build and install the binaries, run the following command.
```bash
go clean ./...
go install ./...
```
The binaries will be located in `$GOPATH/bin`, including `influxd`, `influxd-meta` and `influxd-ctl`.

Or specify the directory of binaries by setting `GOBIN`.

```
GOBIN=$(PWD)/build go install ./...
```

To set the version and commit flags during the build pass the following to the **install** command:

```bash
-ldflags="-X main.version=$VERSION -X main.branch=$BRANCH -X main.commit=$COMMIT"
```

where `$VERSION` is the version, `$BRANCH` is the branch, and `$COMMIT` is the git commit hash.

If you want to build packages, see `build.py` usage information:

```bash
python build.py --help

# Or to run the build utility via Docker
bash build.sh --help

# Or to build a package for your current system
bash build.sh --package

# Or to build all release packages by specifying the platform, arch, branch and version
bash build.sh --platform all --arch all --branch master --version 1.8.11-c1.2.0 --clean --release --package
```

To run the tests, execute the following command:

```bash
go test -v ./...

# run tests that match some pattern
go test -run=TestDatabase . -v

# run tests and show coverage
go test -coverprofile /tmp/cover . && go tool cover -html /tmp/cover
```

To install go cover, run the following command:
```
go get golang.org/x/tools/cmd/cover
```

Build and Push Multi-Arch Docker Images
-----------------

To build and push multi-arch docker images, run the following command:

```bash
INFLUXDB_VERSION=1.8.11-c1.2.0
docker buildx create --name multi-platform --driver docker-container --use
cd docker/data
docker buildx build --platform linux/amd64,linux/arm64 --build-arg INFLUXDB_VERSION=${INFLUXDB_VERSION} --push -f Dockerfile -t chengshiwen/influxdb:${INFLUXDB_VERSION}-data .
docker buildx build --platform linux/amd64,linux/arm64 --build-arg INFLUXDB_VERSION=${INFLUXDB_VERSION} --push -f Dockerfile_alpine -t chengshiwen/influxdb:${INFLUXDB_VERSION}-data-alpine .
cd ../meta
docker buildx build --platform linux/amd64,linux/arm64 --build-arg INFLUXDB_VERSION=${INFLUXDB_VERSION} --push -f Dockerfile -t chengshiwen/influxdb:${INFLUXDB_VERSION}-meta .
docker buildx build --platform linux/amd64,linux/arm64 --build-arg INFLUXDB_VERSION=${INFLUXDB_VERSION} --push -f Dockerfile_alpine -t chengshiwen/influxdb:${INFLUXDB_VERSION}-meta-alpine .
```

Generated Google Protobuf code
-----------------
Most changes to the source do not require that the generated protocol buffer code be changed. But if you need to modify the protocol buffer code, you'll first need to install the protocol buffers toolchain.

First install the [protocol buffer compiler](https://developers.google.com/protocol-buffers/
) 3.x or later for your OS:

Then install the go plugins:

```bash
go get github.com/gogo/protobuf/proto
go get github.com/gogo/protobuf/protoc-gen-gogo
go get github.com/gogo/protobuf/gogoproto
```

Finally run, `go generate` after updating any `*.proto` file:

```bash
go generate ./...
```
**Troubleshooting**

If generating the protobuf code is failing for you, check each of the following:
* Ensure the protobuf library can be found. Make sure that `LD_LIBRARY_PATH` includes the directory in which the library `libprotoc.so` has been installed.
* Ensure the command `protoc-gen-gogo`, found in `GOPATH/bin`, is on your path. This can be done by adding `GOPATH/bin` to `PATH`.


Generated Go Templates
----------------------

The query engine requires optimized data structures for each data type so
instead of writing each implementation several times we use templates. _Do not
change code that ends in a `.gen.go` extension!_ Instead you must edit the
`.gen.go.tmpl` file that was used to generate it.

Once you've edited the template file, you'll need the [`tmpl`][tmpl] utility
to generate the code:

```sh
$ go get github.com/benbjohnson/tmpl
```

Then you can regenerate all templates in the project:

```sh
$ go generate ./...
```

[tmpl]: https://github.com/benbjohnson/tmpl


Pre-commit checks
-------------

We have a pre-commit hook to make sure code is formatted properly and vetted before you commit any changes. We strongly recommend using the pre-commit hook to guard against accidentally committing unformatted code. To use the pre-commit hook, run the following:
```bash
    cd $GOPATH/src/github.com/chengshiwen/influxdb-cluster
    cp .hooks/pre-commit .git/hooks/
```
In case the commit is rejected because it's not formatted you can run
the following to format the code:

```
go fmt ./...
go vet ./...
```

To install go vet, run the following command:
```
go get golang.org/x/tools/cmd/vet
```

NOTE: If you have not installed mercurial, the above command will fail.  See [Revision Control Systems](#revision-control-systems) above.

For more information on `go vet`, [read the GoDoc](https://godoc.org/golang.org/x/tools/cmd/vet).

Profiling
-----
When troubleshooting problems with CPU or memory the Go toolchain can be helpful. You can start InfluxDB Cluster with CPU and memory profiling turned on. For example:

```sh
# start influx with profiling
./influxd -cpuprofile influxdcpu.prof -memprof influxdmem.prof
# run queries, writes, whatever you're testing
# Quit out of influxd and influxd.prof will then be written.
# open up pprof to examine the profiling data.
go tool pprof ./influxd influxd.prof
# once inside run "web", opens up browser with the CPU graph
# can also run "web <function name>" to zoom in. Or "list <function name>" to see specific lines
```
Note that when you pass the binary to `go tool pprof` *you must specify the path to the binary*.

If you are profiling benchmarks built with the `testing` package, you may wish
to use the [`github.com/pkg/profile`](github.com/pkg/profile) package to limit
the code being profiled:

```go
func BenchmarkSomething(b *testing.B) {
  // do something intensive like fill database with data...
  defer profile.Start(profile.ProfilePath("/tmp"), profile.MemProfile).Stop()
  // do something that you want to profile...
}
```

Distributions
-----

You can build distributions such as `.deb` and `.rpm` files using the scripts in the `releng` directory.

For example, we'll build a distribution for 64-bit Linux.
From the `influxdb-cluster` source directory, first build the source tarball:

```sh
$ ./releng/source-tarball/build.bash -p `pwd` -s $COMMIT -b $BRANCH -v vX.Y.Z -o $OUTDIR/src
```

Then build the raw binaries:

```sh
$ GOOS=linux GOARCH=amd64 ./releng/raw-binaries/build.bash -i $OUTDIR/src/influxdb-cluster-src-$COMMIT.tar.gz -o $OUTDIR/bin
```

Then build the full distribution using the source and binary tarballs:

```sh
$ ./releng/packages/build.bash -s $OUTDIR/src/influxdb-cluster-src-$COMMIT.tar.gz -b $OUTDIR/bin/influxdb-cluster_bin_linux_amd64-$COMMIT.tar.gz -O linux -A amd64 -o $OUTDIR/dist
```

You should find your `.deb`, `.rpm`, and `.tar.gz` distribution files in the `$OUTDIR/dist` directory.
