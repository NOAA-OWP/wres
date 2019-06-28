How to create your SSH keys

To create your ssh keys, do the following:

0.  Ensure that you have access to the WRES Deployment Platform

ssh <user.name>@***REMOVED***wres-ti02.***REMOVED***.***REMOVED***

The first time you access the WRES Deployment Platform VM, you will need to add it to your known hosts file; press 'Y' when it asks.  If you cannot access the VM, please let me know.

1.  Ensure that the .ssh directory exists within your home directory with permissions 700:

cd ~
mkdir .ssh
chmod 700 .ssh

2.  Create the three sets of keys:

cd .ssh
ssh-keygen -t rsa
ssh-keygen -t dsa
ssh-keygen -t ecdsa

This will create three *.pub files within your .ssh directory.

3.  Create your authorized_keys file with appropriate contents and set its permissions:

cat *.pub >> authorized_keys
chmod g-rwx,o-rwx authorized_keys
chmod -x authorized_keys
chmod g-w,o-w known_hosts

4.  Test your ability to ssh without providing a password:

ssh <user.name>@***REMOVED***wres-ti02.***REMOVED***.***REMOVED***

If you encounter any problems with any step above, let me know.
