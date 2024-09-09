# 1️⃣🐝🏎️ The One Billion Row Challenge

Forked and mangled into a Go app. See https://1brc.dev/

## Running the Challenge

This repository contains two programs:

* `createmeasurements.go` (invoked via _create\_measurements.sh_): Creates the file _measurements.txt_ in the root directory of this project with random measurement values
* `main.go`: Calculates the average values for the file _measurements.txt_

Execute the following steps to run the challenge:

1. Create the measurements file with 1B rows (just once):

    ```
    go run cmd/generate/main.go 1000000000
    ```

    This will take a minute.
    **Attention:** the generated file has a size of approx. **13 GB**, so make sure to have enough diskspace.

2. Calculate the average measurement values:

    ```
    go build -o solution cmd/solution/*.go
    ./solution
    ```

# Rules and limits

Who knows at this point. Personal rules for my own non-submitting journey:

- Using libraries, since I'm still evaluating stuff. Their algorithms can always be assimilated if need be.
- Keeping input name collision chance below "hit by meteor" level. I.e. an identity hash needs at least 90-ish bits.
- Reading from RAM drive.

## Entering the Challenge

To submit your own implementation to 1BRC, follow these steps:

* Figure it out.

## License

This code base is available under the Apache License, version 2.
