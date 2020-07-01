# Derecho Java wrapper [TODO: more details. Explain the key java classes. use text from report.]
This is an experimental project testing the feasibility of Java API for Derecho, which is written in pure C++.

# Prerequisites [TODO: more details]
1. Derecho
Please follow the [Derecho document](https://github.com/Derecho-Project/derecho) to install Derecho from source code.
2. JDK
`apt install -y default-jdk openjdk-11-jdk`

# Build [TODO: adapt to derecho repo ]
1. Clone this repository
2. Inside `Derecho_Toy`, run `mkdir build && cd build`
3. `cmake .. -DCMAKE_PREFIX_PATH=<derecho-installation-dir>`
4. `make`
5. To run the library, use the following script:
```
#!/bin/bash
export LD_LIBRARY_PATH=<Derecho_Toy build dir>:<derecho build dir>

java -jar <Derecho_Toy build dir>/derecho.jar [number of nodes] [num_senders_selector] [num_messages] [delivery_mode] [message_size]
```

# Test [TODO: follow cascade document]

# Limitations and Future Work [TODO: can use text from report.]
