# 🪞 Refractor

Refractor is linux mirror load-balancer, which parallelizes requests between an extremely dynamic pool of mirrors. Mirrors in the pool are constantly monitored for throughput, and slowest mirrors are continuously rotated out of the pool and replaced by new ones obtained at random.

## Working principle

The core of Refractor is a pool of workers, to which HTTP requests are routed. A worker draws a random mirror from a list, and proxies the response to the user.

Refractor aims to work in a stateless, self-balancing way. It tries to achieve this by picking up mirrors from a large list (referred as a Provider), and routing requests to them while measuring how the mirrors perform. If a mirror is among the bottom N performers, it gets rotated out of the pool. Mirrors that fail to complete requests in a given time are also immediately rotated out, while mirrors that perform above a given threshold are never rotated out even if they are among the bottom performers. After a certain amount of requests, this should stabilize in a pool of fast mirrors.

In an attempt to maximize downlink and speed up the rotation of slow mirrors, requests are split up in several chunks of a configurable size, typically a few megabytes, that are themselves routed to different mirrors. Chunks are buffered in memory and served to clients in a pipelined fashion. If a mirror returns an error for a chunk, or fails to download the chunk in time, the mirror that failed is immediately rotated out and the chunk is re-queued to another mirror.

Mirror throughput is measured using a rolling average, so if a mirror performed well in the past but doesn't anymore, for example because it is currently dealing with a large amount of traffic, it gets rotated out.

## Usage

The provided docker image can be run directly with no arguments and it will use the default config (`refractor.yaml`).

```shell
docker run ghcr.io/roobre/refractor:$VERSION
```

The default config will spin up Refractor to load-balance across Archlinux mirrors located in western Europe. To serve mirrors from different regions, check out the provider configuration below.

The updated config file can be mounted in the docker container in `/config/refractor.yaml`.

## Providers

Refractor is designed to be distribution-agnostic, as long as a Provider that can fetch a mirror and feed it to the pool is implemented. As refractor automatically keeps fast mirrors and discards slow ones, providers do not need to sort or benchmark mirrors before supplying them to the pool.

It is recommended, however, for providers to apply coarse-grain filter such as physical location, as doing so will allow the pool to stabilize faster.

For the moment, the following providers exist:

### Arch Linux (`archlinux`)

The Arch Linux provider feeds mirrors from `https://archlinux.org/mirrors/status/json/`, after applying some user-defined filters. For now, filtering by country and by score is allowed.

```yaml
provider:
  archlinux:
    maxScore: 5
    countries:
      - ES
      - IT
      - FR
      - PT
```

### Command (`command`)

The Command provider allows to feed to the pool mirror URLs obtained from running an user-defined command. This should help as an stop-gap for supporting distros without coding providers from them.

> ⚠️ Refractor rotates mirrors from the pool very aggressively, which means the specified command will be called multiple times and very often. Please make sure this command is not hammering any public API without appropriate caching.

```yaml
provider:
  command:
    #shell: /bin/bash # Defaults to $SHELL, then to /bin/sh
    command: |
      cat <<EOF | sort -R | head -n 1
        http://foo.bar
        http://example.local
        https://another.mirror
      EOF
```

The specified command is expected to return a single line containing the mirror URL. If more than one line is printed, Refractor will emit a warning and ignore the rest. Refractor will echo the command's standard error as log lines with `warning` level.

### Implement your own!

Providers are very easy to implement in-code, as they only need to be able to retrieve a random mirror from a list.

```go
type Provider interface {
	Mirror() (string, error)
}
```

As an example, the Arch Linux mirror provider retrieves the list of mirrors from `https://archlinux.org/mirrors/status/json/`, applies some user-defined country and score settings, and returns a random mirror from the resulting list.

Implementing providers in code is encouraged as it provides maximum flexibility to control caching and configuration options. PRs are welcome!

## Trivia

- The name "Refractor" is a gimmick to [Reflector](https://wiki.archlinux.org/title/Reflector)
- Refractor is similar to [flexo](https://github.com/nroi/flexo), with a more aggressive mirror switching strategy
- The author does in fact like tests, but they're short on vacation days

## Project status

Currently this project is in _indev_ stage, meaning it is currently being tested on my local machine. Contributions are welcome, especially if you get in touch with me by opening an issue or sending me an email.
