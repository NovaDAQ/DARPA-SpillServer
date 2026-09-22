# DARPA Spill Information Server

This is a new application that will run on the gateway server novadaq-near-gateway-01.fnal.gov  the purpose of the application is to provide a web application that will be able to server information on different beam and accelerated event time stamps to clients that connect.

The information that is presented by this server is obtained by making a query to a very litle weight embedded bottle server that running on a custom piece of hardware called a TDU.  The structure of this server can be found in TDUWeb package.  This package provide different routes which run a specialized set of commands that get information from the memory systems of the hardware.  The specific programs being run are built from the SHM_Utilities package and from the TDUControl and TDUUtilities packages.

# Application flow

The server application should query the TDUWeb routes on a regular basis to obtain specific information regarding the accelerator events that have been recorded by the hardware and pushed into the shared memory systems of the device.  The goal is to get a copy of the decoded events and their time stamps.  The server application should store they locally so that they can be queried by a client either through a web interface or through an API.

The clients will want the information retrieved as a structured data table.  They will want to be able to select a time range (start and end times) and an accelerator event number in a custom hex format.  For example the acclerator event "8F" is denoted as "$8f" or the signal "74" is "$74".  There are a number of different signals that are decoded and these are documented in the Spill Server, TDUWeb, TDUUtilities, SHM_Utilities, and other packages that have been provided.

For example the client will want to be able to retrieve all the time stamps for the $74 events between Jul 1, 2026 and today.  Or from 09:15 today until 11:34 today.  The time stamps are in a special custom timebase called NOvA time.  There is a library which will convert from this timebase to other more common bases.  The nova-time-decoder package provides conversion functions.  The most common formats that the client will want are GPS and UTC.  

# Configuration

Everything should be configurable from the commandline or from a yaml file.

# Documentation

Full documentation on how to build, start, run, and configure the application should be generated.  There should also be man pages generated for all applications.

# Source Data

The custom TDU hardware is accessible only from within the NOVA DAQ Network.  The gateway node is on this network.  The web address of the data source is http://tdu-near-master-ppc-01   on port 8080.  So curl "http://tdu-near-master-ppc-01:8080/tcr_status"
 should give a status response in json format.  

# Output Formats

The clients will want data in the following formats: CSV, JSON


# Authentication

Start with no authentication on the server, but put in the correct hooks for enabling OIDC single sign on support (using the Fermilab Single sign on system).  


