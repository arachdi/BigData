using System;
using System.IO;

namespace BigData
{
	
	public class BigDataMain
	{

		public static void Main (string[] args)
		{
			// Reading program arguments:
			// 1) mode (1 : master, 2 : slave)
			// 2) path of the input directory
			// 3) configuration file

			string inpDir = null;      // directory where files are located
			string confFile = null;    // path frothe master config. file.
			int portNumber = 0;  // port number for slave process
	
			// run program in Master mode
			if (args [0] == "master") 
			{
				
				inpDir = args [1];
				confFile = args [2];

				// runs as master process

				MasterProcess master = new MasterProcess (inpDir, confFile);

				master.Start ();
			} 

			// run program in Slave mode
			else if (args [0] == "slave") 
			{
				// runs as a slave process

				portNumber = Convert.ToInt32 (args [1]);

				SlaveProcess slave = new SlaveProcess (portNumber);

				slave.Start ();

			} 

			else 
			
			{
				Console.WriteLine ("Wrong arguments! Try again!");
			}
				
		}

	}
}
