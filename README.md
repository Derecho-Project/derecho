# Derecho
This is the main repository for the Derecho project. It unifies the RDMC, SST, and Derecho modules under a single, easy-to-use repository. 

## Organization
The code for this project is split into modules that interact only through each others' public interfaces. Each module resides in a directory with its own name, which is why the repository's root directory is empty. External dependencies are located in the `third_party` directory, and will be pointers to Git submodules whenever possible to reduce code duplication. 
