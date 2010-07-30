Mark's Fork notes:
==================

IMPORTANT: This fork requires the Reactive Framework to be installed.

The Samples solution in this fork includes an Azure Cloud project with a Worker Role
which you can deploy to a Hosted Service.

To get this working, I've had to implement my own TCP-based HTTP stack for the Raven
server. There may be issues with this which have not turned up during my basic testing.
If you have any problems, please use the Github Issues feature to report them.

Also, there is no authentication on this implementation. The Role is intended to be used
internally as storage for a Web application running in the same deployment, although it
is currently configured as externally facing for testing purposes.

Finally, remember to change the Storage connection strings on the CloudRaven project to
point to your own storage.

Raven DB
========
This release contains the following:

/Server - The files required to run Raven in server / service mode.
		  Execute /Server/RavenDB.exe /install to register and start the Raven service
		  
/Web	- The files required to run Raven under IIS.
		  Create an IIS site in the /Web directory to start the Raven site.

/Client-3.5
		- The files required to run the Raven client under .NET 3.5
		
/Client
		- The files required to run the Raven client under .NET 4.0
		*** This is the recommended client to use ***

/ClientEmbedded
		- The files required to run the Raven client, in server or embedded mode.
		  Reference the RavenClient.dll and create a DocumentStore, passing a URL
		  or a directory.

/Bundles
    - Bundles that extend Raven in various ways
    
/Samples
    - The sample applications for Raven
    * Under each sample application folder there is a "Start Raven.cmd" file which will
      starts Raven with all the data and indexes required to run the sample successfully.
    
/Raven.Smuggler.exe
    - The Import/Export utility for Raven
		  
You can start the Raven Service by executing /server/ravendb.exe, you can then visit
http://localhost:8080 for looking at the UI.

For any questions, please visit: http://groups.google.com/group/ravendb/

Raven's homepage: http://ravendb.net
