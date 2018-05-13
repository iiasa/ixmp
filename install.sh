#!/bin/bash

inst_args=$@

function error_msg() {
	echo =====================================================
	echo  There was an error during the install process!
	echo =====================================================
	read -p "Press any key to continue..."
	exit 1
}

# install ixmp
python setup.py install $inst_args
if [ "$?" -ne "0" ]; then
    error_msg
fi
py.test tests

echo 
echo Installation complete
echo
read -p "Press any key to continue..."
echo
exit 0
