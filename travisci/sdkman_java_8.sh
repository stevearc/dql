#!/bin/bash

# nosetests dynamo plugin requires Java 8 or order to work properly.
# However, TravisCI's xenial ships with Java 11 and Java can't be set
# with `jdk` when python is selected as language.
# Other ways of setting the jdk have not panned out.
# So, had to use sdkman.

echo "---------------------------------------"
echo "Installing sdkman ....................."
echo "---------------------------------------"
curl -s "https://get.sdkman.io?rcupdate=false" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
echo "---------------------------------------"
sdk version
echo "---------------------------------------"
sdk list java
echo "---------------------------------------"


echo "---------------------------------------"
echo "Installing java using sdkman..........."
echo "---------------------------------------"
sdk install java 8.0.265.hs-adpt < /dev/null
sdk current java
echo "---------------------------------------"
echo "java -version"
java -version
echo "---------------------------------------"
