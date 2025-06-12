# syntax=docker/dockerfile:1

FROM ubuntu:24.04 AS base

# Install build tools and distro-packaged libraries
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libtool \
    doxygen \
    libssl-dev \
    libspdlog-dev

# Stage for building dependencies, so their sources aren't included in the final image
FROM base AS dep-build

# Run all of the "install prerequisite" scripts, which
# compile and install the libraries into /usr/local/
COPY scripts/prerequisites /prerequisites
WORKDIR /prerequisites
RUN ./install-json.sh
RUN ./install-libfabric.sh
RUN ./install-mutils.sh
RUN ./install-mutils-containers.sh

# Final stage for Derecho development; leaves Derecho source code in the image so it can be edited
FROM base AS derecho-dev

# Copy everything in the dep-build stage's /usr/local/ to get the compiled libraries
COPY --from=dep-build /usr/local/ /usr/local/

# Copy in all the Derecho source code
WORKDIR /derecho
COPY . .

# Build initially in Debug mode, since this image is for Derecho development
RUN mkdir build-Debug && cd build-Debug \
    && cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON .. \
    && cmake --build . -j $(( $(nproc) - 1))

# Set the default command to a shell for interactive development
CMD ["/bin/bash"]

# Alternate, intermediate stage for building Derecho as a library and installing it
FROM base AS derecho-build

# Copy compiled libraries from dep-build stage
COPY --from=dep-build /usr/local/ /usr/local/

# Copy in Derecho source code
WORKDIR /derecho
COPY . .
# Build and install in Release With Debug Info mode
RUN mkdir build-RelWithDebInfo && cd build-RelWithDebInfo \
    && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. \
    && cmake --build . -j $(( $(nproc) - 1)) \
    && cmake --install .

# Alternate final stage for using Derecho as a library to develop something else.
# Only includes the installed library, not the source code, assuming this will be
# used as the base for another image.
FROM base AS derecho-lib

# Copy dependencies built in the dep-build stage
COPY --from=dep-build /usr/local/ /usr/local/
# Copy installed Derecho from the derecho-build stage
COPY --from=derecho-build /usr/local/ /usr/local/

