#!/bin/bash
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
sdk current javasdk
echo "---------------------------------------"
java -version
echo "---------------------------------------"
