# Operations and Deployment on novadaq-near-gateway-01.fnal.gov

We need to setup a stable deployment of the system on novadaq-near-gateway-01.fnal.gov

The deployment will run as the user "novadaq" but will need my kerberos credentials for certain parts of the activity.  My kerberos principal is anorman@FNAL.GOV and if there isn't a valid ticket, then one should be generated.  I have stored the password for this ~/Credentials/Kerberos/anorman-kerb.txt

The area that should be used for deployment of the Fermi Data Platform parts of this project (FDP) is anorman-fdp on the novadaq-near-gateway-01 machine.  That area already has the Fermi-Data-Platform-Tools, Fermi-SSO-AccessLib and htgettoken packages installed in it.  You should look at the installation to understand how to work with its environment

# Data Organization

When dumping data the data should be written to the /data/DARPA-spill-data/ area

There is a directory for raw data called "raw" and for data that has the time stamps shifted called "shifted"

Under those areas the data should be organized by month (aug, sep, oct, nov, dec, jan, feb, mar, apr) and then by day under those areas.

Data from the darpa-spill-dump utility should go into the raw areas and be split by day and hour.

# Workflow

The following things should be done on the gateway machine:

* Data should be dumped from the server into a file for just the $74 and $A9 events.  Data should be split by hour and day.
* Data should be dumped from the server into a file for ALL accelerator events.  The data should be split by hour and day.
* Data should be dumped from the server into a file for just the booster beam related events.  The data should be split by hour and day.

Once a day you should generate a file for the preceeding day (a single file with 24 hours of data)
Every hour you should generate a file for the preceeding hour, for example if you are running this at 13:05 you would generate the file for times 12:00  <= t < 13:00

After a file is generated it should be time shifted using the darpa-spill-shift utility.  
Generate a token to use for this with a gaussian with width 50us.  Store the token in anorman-fdp/tokens. Use a different token each month.

Upload the files to the Fermi Data Platform.  The token to use for this is the amsc issuer with the novaendwrite role

Use the Fermi SSO for auth.  The user and password for SSO should be in the .env file.  Create the file with placeholder values.  I will then fill them in when you ask me to.

Upload the data to the amsc/novaend area on the Fermi Data Platform.  Keep the same directory structure.  Then separately upload the time shifted data to the public area on amsc.

# Status views

Make and update a webpage with the status of the latest transfers.  The webpage should be hosted from the novadaq-near-gateway-01 machine.

# Other

Ask me questions about additional things you need.