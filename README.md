## Solution

I was fortunate enough to reach this level early, and pass it before Stripe improved octopus. As such I passed it with [my first attempt](https://github.com/danielfone/stripe-ctf-level4/commit/10be54c6915d1492791bb147a0c9daeb3c27003d), simply by removing the failover (node0 becomes Single Point of Failure) and forwarding requests from the secondaries to the primary.

The remaining commits are my futile attempts to pass this level after the SPOF check was added to the tests. Looking at the code for successful solutions, I can see I was on the right path but needed to invest more time into making my command forwarding more robust.

---

# SQLCluster

SQLCluster makes your SQLite highly-available.

## Getting started

To run this level, you'll need a working Go installation. If you don't
have one yet, it's quite easy to obtain. Just grab the appropriate
installer from:

  https://code.google.com/p/go/downloads/list

Then set your GOPATH environment variable:

  http://golang.org/doc/code.html#GOPATH

It'll probably be convenient to check this code out into
$GOPATH/src/stripe-ctf.com/sqlcluster (that way, `go build` will know
how to compile it without help). However, you can use the provided
`build.sh` regardless of where you happened to check this level out.

## Building and running

Run `./build.sh` to build the SQLCluster binary.

As always, you can run test cases via `test/harness`. This will
automatically fetch and compile Octopus, download your test cases, and
score your level for you.

Octopus will print out your score, together with how it arrived at
said score.

## Protocol

SQCluster communicates with the outside world over HTTP. The public
interface is simple:

  POST /sql:

    input:  A raw SQL body

    output: A message with the form "SequenceNumber: $n", followed
            by the output of that SQL command.

Run `./build.sh` to build SQLCluster and have it print out some
example usage (including `curl`s you can run locally).

## Supported platforms

SQLCluster has been tested on Mac OSX and Linux. It may work on other
platforms, but we make no promises. If it's not working for you, see
https://stripe-ctf.com/about#development for advice on getting a
development environment similar to our own.
