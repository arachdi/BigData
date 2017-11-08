using System;
using System.Net.Sockets;
using System.Net;
using System.IO;
using System.Security.AccessControl;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace BigData
{
	/// <summary>
	/// Utility class for handling socket states
	/// in asynchronous sockets event handlers
	/// </summary>
	public class StateObject
	{
		// Client socket.
		public Socket workSocket = null;

		public int bufferSize = 10024;

		// Receive buffer.
		public byte[] buffer = new byte[10024];

		public bool firstCall = true; // used when state is repeatedly passed to socket asynch
		                              // callback

		public long dataSize =0; // used to store how much data has been sent/received so far
	}

	/// <summary>
	/// Utility class for serializing / deserializing SimilarFiles list
	/// created by slave process after comparing files. Created byte[] will 
	/// send over socket. Similarly deserialized object will be processed by master 
	/// to get the results of comparing one file to a set of files.  
	/// </summary>
	public static class SimilarFilesListSerializer
	{
		/// <summary>
		/// Serialize SimilarFiles list to Bute array
		/// </summary>
		/// <param name="items">Items.</param>
		public static byte[] Serialize(this IEnumerable<BigData.SlaveProcess.SimilarFiles> items)
		{
			using (MemoryStream m = new MemoryStream()) 
			{
				using (BinaryWriter writer = new BinaryWriter(m, System.Text.Encoding.UTF8, true)) 
				{
					foreach (var item in items) 
					{
						var itemBytes =  BigData.SlaveProcess.SimilarFiles.Serialize(item);
						writer.Write(itemBytes.Length);
						writer.Write(itemBytes);
					}
				}

				return m.ToArray();
			}
		}

		/// <summary>
		/// Deserialize byte array to SimilarFiles list
		/// </summary>
		/// <param name="data">Data.</param>
		public static List<BigData.SlaveProcess.SimilarFiles> Deserialize(byte[] data)
		{
			var ret = new List<BigData.SlaveProcess.SimilarFiles>();

			using (MemoryStream m = new MemoryStream(data))
			{
				using (BinaryReader reader = new BinaryReader(m, System.Text.Encoding.UTF8)) 
				{
					while (m.Position < m.Length)
					{
						var itemLength = reader.ReadInt32();
						var itemBytes = reader.ReadBytes(itemLength);
						var item = BigData.SlaveProcess.SimilarFiles.Deserialize(itemBytes);
						ret.Add(item);
					}
				}
			}

			return ret;
		}

	}

	/// <summary>
	/// Slave process Class definition.
	/// </summary>
	public class SlaveProcess
	{

		/// <summary>
		/// Using assynchronous sockets to send data between M & S.
		/// These events are used to coordinate threads
		/// </summary>


		static System.Threading.AutoResetEvent waitAccept = new System.Threading.AutoResetEvent(false);
		static System.Threading.AutoResetEvent waitReceive = new System.Threading.AutoResetEvent(false);


		static byte[] receiveBuffer = new byte[10024]; // receive buffer for asynchronous sockets


		static Socket handler = null;
		static Socket listener = null;

		int portNumber = 0;

		// used for debugging
		static bool verbose = true;


		// added this timer to re-send messages to master
		public static System.Timers.Timer sentCmpResTimer = new System.Timers.Timer(2000);

		/// <summary>
		/// class identifying file within block
		/// with block index & file index 
		/// </summary>
		[Serializable]
		public class FileID
		{
			public int blockIdx = 0;   // block index
			public int blkfileIdx =0; // file index within block

			/// <summary>
			/// Overriding Serialize to save to stream
			/// </summary>
			/// <param name="obj">Object.</param>
			public static byte[] Serialize(FileID obj)
			{
				byte[] bytes = null;

				using (var stream = new MemoryStream())
				{
					using (var writer = new BinaryWriter(stream))
					{
						writer.Write(obj.blockIdx);
						writer.Write(obj.blkfileIdx);
					}

					bytes = stream.ToArray();
				}

				return bytes;
			}
			/// <summary>
			/// Overriding Deserialize to read from stream
			/// </summary>
			/// <param name="data">Data.</param>
			public static FileID Deserialize(byte[] data)
			{
				var obj = new FileID();

				using (var stream = new MemoryStream(data))
				{
					using (var reader = new BinaryReader(stream))
					{
						obj.blockIdx= reader.ReadInt32();
						obj.blkfileIdx = reader.ReadInt32();
					}
				}

				return obj;
			}

			// overriden so FileID object can be used in collections
			override public bool Equals(object other) 
			{
				var otherFoo = other as FileID;
				if (otherFoo == null)
					return false;
				return (blockIdx==otherFoo.blockIdx && blkfileIdx ==otherFoo.blkfileIdx);
			}

			override public int GetHashCode()
			{
				return 17 * blockIdx.GetHashCode () + blkfileIdx.GetHashCode ();
			}

		}
		/// <summary>
		/// class used to identify similar files when sending result back to Master
		/// </summary>
		[Serializable]
		public class SimilarFiles
		{
			public FileID fileID1;  // file Identifiers (block index & file index)
			public FileID fileID2;  // for 2 similar files

			/// <summary>
			/// Overriding Serialize to save to stream
			/// </summary>
			/// <param name="obj">Object.</param>
			public static byte[] Serialize(SimilarFiles obj)
			{
				byte[] bytes = null;

				IFormatter formatter = new BinaryFormatter();
				using (MemoryStream stream = new MemoryStream())
				{
					formatter.Serialize(stream, obj);
					bytes = stream.ToArray();
				}

				return bytes;
			}
			/// <summary>
			/// Overriding Deserialize to read from stream
			/// </summary>
			/// <param name="data">Data.</param>
			public static SimilarFiles Deserialize(byte[] data)
			{
				SimilarFiles obj;

				MemoryStream memStream = new MemoryStream();
				BinaryFormatter binForm = new BinaryFormatter();
				memStream.Write(data, 0, data.Length);
				memStream.Seek(0, SeekOrigin.Begin);
				obj = (SimilarFiles) binForm.Deserialize(memStream);

				return obj;
			}
		}
		// keeps track of blocks & files in blocks
		// Hashtable key is FileID objects 
		// Hashtable value is string of filename

		static Hashtable blocks = new Hashtable ();

		// Used to store initial block information received from Master
		// This information is used for checking that all files have been
		// successfully received from master

		static int[] blockInfo = null;

		// Used to store list of similar files after comparing one file
		// to all files in one block

		static List<SimilarFiles> similarFiles = new List<SimilarFiles> ();

		// stores the next command to be executed by slave after
		// receiving intructions from Master
		static MasterProcess.Command nextCmd;

		// Set of static variables used in socket receive
		// when recursively receiving files from Master process

		static int cmd;
		static int currentBlockIdx = 0;
		static int currentFileIdx = 0;
		static int currentFileIndex = 0;
		static long currentFileSize = 0;
		static string currentHashCode = null;
		static string currentPathName = null;
		static string currentFileName = null;


		// Set of static variables used to store
		// index of current file to be compared to all
		// files in current indentified block

		static int currentFileCmpIdx = 0;
		static int currentFileCmpBlkIdx = 0;
		static int currentBlkCmpIdx = 0;

		static bool keepReceiveFile = false;
		static bool logOnce = true;
		static bool delOldDir = true;

		// used for debugging
		static int bn = 0; // nb of received blocks of data for same file since large files are received & copied recursively by slave 
		static StreamWriter log; // log file
		static StreamWriter logTest = new StreamWriter("TestLog.txt");

		static long nbOfComparaisons = 0;

		static private System.Object lockRecAsyncReceive = new System.Object(); // mutex  variable for coordinating access to 
		                                                                        // asynch socket receive call back from different threads

		/// <summary>
		/// Initializes a new instance of the <see cref="BigData.SlaveProcess"/> class.*/
		/// </summary>
		/// <param name="portNb">Port nb.</param>
		public SlaveProcess (int portNb)
		{
			portNumber = portNb;
				
			log = new StreamWriter("SlaveLog.txt");

			sentCmpResTimer.Elapsed += OnTimedEvent;
	
		}

		/// <summary>
		/// Starts a slave process.
		/// </summary>
		public void Start()
		{
			
			Console.WriteLine("Slave process has started\n");
			log.WriteLine ("Slave process has started\n");

			StartListening ();	
		
			Receive ();
		
		}

		/// <summary>
		/// Creates the socket & waits for Master to connect
		/// </summary>
		void StartListening()
		{
			IPAddress ipAddress = null;

			var host = Dns.GetHostEntry(Dns.GetHostName());

			foreach (var ip in host.AddressList)
			{
				if (ip.AddressFamily == AddressFamily.InterNetwork)
				{
					ipAddress = ip;
					break;
				}
			}

			IPEndPoint hostEndPoint = new IPEndPoint(ipAddress, portNumber);

			listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

			try
			{
				listener.Bind(hostEndPoint);

				listener.Listen(-1);

				Console.WriteLine("Waiting for Master...\n");
				log.WriteLine ("Waiting for Master...\n");

				waitAccept.Reset();
			
				listener.BeginAccept(new AsyncCallback(AcceptCallback), listener);

				// wait for Master to conenct before continuing

				waitAccept.WaitOne();
			
			}

			catch (Exception e)
			{
				Console.WriteLine(e.ToString());
			}

		}
			
		/// <summary>
		/// Receive loop for Slave
		/// </summary>
		void Receive()
		{
			StateObject state = new StateObject();
			state.workSocket = handler;

			bool receive = true;

			// NOW begin receiving data

			while (receive) 
			{
				handler.BeginReceive (state.buffer, 0, state.bufferSize, 0, new AsyncCallback (AsyncReceiveCallBack), state);

				// wait for next message
				waitReceive.WaitOne ();

				// send result or coordination nesage to Master
				SendToMaster (nextCmd);

				if (nextCmd == MasterProcess.Command.Info) 
				{
					receive = false;
					logTest.Close ();
					Disconnect();
					
				}

			}
			
		}
		/// <summary>
		/// Accept connection callback.
		/// </summary>
		/// <param name="ar">Ar.</param>
		private static void AcceptCallback(IAsyncResult ar)
		{
			listener = (Socket)ar.AsyncState;
			handler = listener.EndAccept(ar);

			Console.WriteLine("Connected to Master\n");
			log.WriteLine ("Connected to Master\n");

			waitAccept.Set();
		}

		/// <summary>
		/// Shutdown & close sockets.
		/// </summary>
		static void Disconnect()
		{
			// Release the socket.

			listener.Close();
		}
			
		/// <summary>
		/// Asyncs the receive callback:
		/// Receives Master instructions & coordination commands.
		/// Receives blocks of files from master.
		/// Recursively & asynchronously creates LARGE files received from master.
		/// </summary>
		/// <param name="ar">Ar.</param>
		private static void AsyncReceiveCallBack(IAsyncResult ar)
		{
			// Retrieve the socket from the state object.

			StateObject state = (StateObject)ar.AsyncState;
			handler = state.workSocket;

			int bytesRead = handler.EndReceive(ar);

			lock (lockRecAsyncReceive) // locking this part of the code to make sure it's accessed by one thread at a time
			{
				using (var memoryStream = new MemoryStream (state.buffer)) 
				{
					// start processing received data

					using (BinaryReader reader = new BinaryReader (memoryStream)) 
					{
						if (!keepReceiveFile)
							cmd = reader.ReadInt32 (); // read cmd to determine next action
						// don't read if it's a recursive call to receive consecutive chunks of files from Master

						if (cmd == (int)MasterProcess.Command.Info) 
						{
							Console.WriteLine ("Received block information message\n");
							log.WriteLine ("Received block information message\n");

							// Read information regarding the structure of the blocks of files to be received from Master
					
							// nb of block of files that Master is going to send

							int nbBlocks = reader.ReadInt32 ();
							blockInfo = new int[nbBlocks];

							// the number of files in each block

							for (int i = 0; i < nbBlocks; i++) 
							{
								blockInfo [i] = reader.ReadInt32 ();
							}

							nextCmd = MasterProcess.Command.CopyFile;

							waitReceive.Set ();

							return;

						}

						if (cmd == (int)MasterProcess.Command.CopyFile) 
						{
							try {
								if (state.firstCall) 
								{
									state.firstCall = false;
									currentBlockIdx = reader.ReadInt32 ();
									currentFileIdx = reader.ReadInt32 ();
									currentFileName = reader.ReadString ();
									currentFileSize = reader.ReadInt64 ();
									currentHashCode = reader.ReadString ();
									currentPathName = CreateDir (currentBlockIdx) + @"/" + currentFileName;

									FileID fileId = new FileID ();
									fileId.blkfileIdx = currentFileIdx;
									fileId.blockIdx = currentBlockIdx;
	

									bn = 0;

									// Adds file information to the hashtable
									// Used later during the comparaison step

									string value = new string(currentPathName.ToCharArray());

									blocks.Add (fileId, value);

									int pos = (int)memoryStream.Position;

									// Creates the file the first time

									using (var writer = new BinaryWriter (File.Open (currentPathName, FileMode.Create))) 
									{
										writer.Write (state.buffer, pos, bytesRead - pos);
										writer.Flush ();
										writer.Close ();
									}

									// keep track of current data received from file

									state.dataSize = bytesRead - pos;

									//debug
									if (verbose) 
									{
										string output = String.Format ("Copying file {0}\n"  +
											                           "Block index {1} : file index {2} : data block index {3}\n"+ 
											                           "Original file index {4}\n", currentFileName, currentBlockIdx, currentFileIdx, bn, currentFileIndex);
										Console.WriteLine (output);
										log.WriteLine (output);
									}


									bn++;

									if (state.dataSize == currentFileSize) 
									{
										if (VerifyFileHashcode (currentHashCode, currentPathName)) 
										{
											if (verbose)
												Console.WriteLine ("Hashcode verification success\n");
										}

										else
										{
											//if doesn't match should keep track of file index & ask master to send again
											// Not implemented
											;
										}
									
										nextCmd = MasterProcess.Command.NextFile;
										keepReceiveFile = false;
										waitReceive.Set ();
										state.firstCall = true;
										currentFileIndex++;

									} 

									else 
									{
										// read more data from socket
										keepReceiveFile = true;
										handler.BeginReceive (state.buffer, 0, state.bufferSize, 0, new AsyncCallback (AsyncReceiveCallBack), state);
									}
									
								} 
								else 
								{   
									// recursively append received data to current stream
									using (var writer = new BinaryWriter (File.Open (currentPathName, FileMode.Append))) 
									{ 
										// append received data to file created previously
										writer.Write (state.buffer, 0, bytesRead);
										writer.Flush ();
										writer.Close ();
									}

									// keep track of current data received from file

									state.dataSize += bytesRead;

									//debug checking that all files are received in chuncks of data & recursively save localy

									if (verbose) 
									{
										string output = String.Format ("Copying file {0}\n"  +
											"Block index {1} : file index {2} : data block index {3}\n"+ 
											"Original file index {4}\n", currentFileName, currentBlockIdx, currentFileIdx, bn, currentFileIndex);
										Console.WriteLine (output);
										log.WriteLine (output);
									}


									if (state.dataSize == currentFileSize) 
									{ // if read the whole file...
										if (VerifyFileHashcode (currentHashCode, currentPathName)) 
										{
											if (verbose)
												Console.WriteLine ("Hashcode verification success\n");
										}
										
										nextCmd = MasterProcess.Command.NextFile; // request the next one
										keepReceiveFile = false;
										waitReceive.Set (); // signal the main receiving thread
										state.firstCall = true;
										currentFileIndex++;
									} 

									else 
									{
										// read more data from socket
										keepReceiveFile = true;
										handler.BeginReceive (state.buffer, 0, state.bufferSize, 0, new AsyncCallback (AsyncReceiveCallBack), state);
									}

									bn++;
								}
							} 

							catch (Exception e) 
							{
								Console.WriteLine (e.Message);
								log.WriteLine (e.Message);
							}

						}							

						if (cmd == (int)MasterProcess.Command.Compare) 
						{
							
							if (logOnce) 
							{
								logOnce = false;
								Console.WriteLine ("Start of files comparaison phase..\n");
								log.WriteLine ("Start of files comparaison phase..\n");
							}

							sentCmpResTimer.Stop();

							// file comparaison command received from Master

							int blockIdx = reader.ReadInt32 (); // block index
							int fileIdx = reader.ReadInt32 ();  // file index in block
							int blkIdxCmp = reader.ReadInt32 (); // identified block


							CompareFiles (blockIdx, fileIdx, blkIdxCmp);

							TestSimilarFiles ();

							nextCmd = MasterProcess.Command.Result;
							waitReceive.Set (); // signal waiting thread

							return;
						}	

						if (cmd == (int)MasterProcess.Command.End) 
						
						{
							sentCmpResTimer.Stop();

							nextCmd = MasterProcess.Command.Info; // sends statistics info to Master
							waitReceive.Set (); // signal waiting thread
						}
					}
				}	
			}
		}
			
		/// <summary>
		/// Creates a new dir in current working dir postfixed with index number
		/// </summary>
		/// <returns>path.</returns>
		/// <param name="blkIdx">block index.</param>
		static string CreateDir(int blkIdx)
		{
			string subDir = null;

			DirectorySecurity securityRules = new DirectorySecurity();
			securityRules.AddAccessRule(new FileSystemAccessRule("Everyone", FileSystemRights.FullControl, AccessControlType.Allow));

			//gets current directory
			string path = Directory.GetCurrentDirectory();

			if (delOldDir) 
			{
				delOldDir = false;

				subDir = "BigData_S";

				path += @"/" + subDir;

				if (Directory.Exists (path)) // removes directories that may have been created in previous runs
				{ 
					Directory.Delete (path, true);
				} 
			}

			subDir = "BigData_S"+"/"+"block" + Convert.ToString(blkIdx);
			path = Directory.GetCurrentDirectory();

			// appends new dir name

			path = path + @"/" + subDir;

			var dest = Directory.CreateDirectory(subDir);
			string output = String.Format("Receiving files from Master and copying them to block{0}\n", blkIdx);
			Console.WriteLine(output);
			log.WriteLine(output);

			return path;
		}

		/// <summary>
		/// Compares the file identified by block index and file index
		/// to all files located in block indentified by block index : blkIdxCmp
		/// Skips file with same index in block because it's the same file
		/// </summary>
		/// <param name="blkIdx">block index</param>
		/// <param name="fileIdx">file index</param>
		/// <param name="blkIdxCmp">block index whose files are to be compared to file.</param>
		static void CompareFiles(int blkIdx, int fileIdx, int blkIdxCmp)
		{
			try
			{
				// store information for current comparaison
				// used by Master to validate comparaison results 

				currentFileCmpIdx = fileIdx;
				currentFileCmpBlkIdx = blkIdx;
				currentBlkCmpIdx = blkIdxCmp;

	
				string[] files = FilesInBlock (blkIdxCmp); // list of files in block whose files will be compared
														   // to current file
				string file = FileInBlock(blkIdx,fileIdx); // current file

				if (verbose) 
				{
					string output = String.Format ("Comparing file in block{0} and index{1}\n" +
						                           "To all files in block{2}\n", blkIdx, fileIdx, blkIdxCmp);
					Console.WriteLine (output);
					log.WriteLine (output);
				}

				// Reset similar files list

				similarFiles.Clear();

				for (int i = 0; i<files.Length; i++) 
				{
					if ((i != fileIdx) || (blkIdx != blkIdxCmp)) // don't compare files with same index & same block index
					{
						if (CompareFiles(file,files[i]))  // if similar add to similar files list
						{
							FileID fileId1 = new FileID();
							FileID fileId2 = new FileID();

							fileId1.blockIdx = blkIdx;
							fileId1.blkfileIdx = fileIdx;

							fileId2.blkfileIdx = i;
							fileId2.blockIdx= blkIdxCmp;

							SimilarFiles simFiles = new SimilarFiles();

							simFiles.fileID1 = fileId1;
							simFiles.fileID2 = fileId2;

							similarFiles.Add(simFiles);
						}
					}
				
				}
			}
			catch(Exception e) 
			{
				Console.WriteLine (e.Message);

			}
		}
		/// <summary>
		/// Returns the filename of the file identified by 
		/// a block index and file index
		/// </summary>
		/// <returns>the file name</returns>
		/// <param name="blockIdx">block index</param>
		/// <param name="fileIdx">file index</param>
		static string FileInBlock(int blockIdx, int fileIdx)
		{
			FileID fileId = new FileID ();
			fileId.blockIdx = blockIdx;
			fileId.blkfileIdx = fileIdx;
			string fileName = (string)blocks [fileId];
			return fileName;
		}

		/// <summary>
		/// Returns the list of filenames in one block
		/// identified by a block id
		/// </summary>
		/// <returns>a lits of filenames (string)</returns>
		/// <param name="blockIdx">Block index.</param>
		static string[] FilesInBlock(int blockIdx)
		{
			string[] files = new string[blockInfo [blockIdx]];
			for (int i = 0; i < blockInfo [blockIdx]; i++) 
			{
				int fileIdx = i;
				FileID fileId = new FileID ();
				fileId.blockIdx = blockIdx;
				fileId.blkfileIdx = fileIdx;
				files [i] = (string)blocks [fileId];
			}

			return files;
		}
		/// <summary>
		/// Compares two files.
		/// </summary>
		/// <returns><c>true</c>, if files was compared, <c>false</c> otherwise.</returns>
		/// <param name="file1">File1.</param>
		/// <param name="file2">File2.</param>
		static bool CompareFiles(string file1, string file2)
		{
			int file1byte;
			int file2byte;
			FileStream fs1;
			FileStream fs2;

			nbOfComparaisons++;

			if (file1 == file2) // same files are not considered similar
			{
				return false;
			}

			// Open the two files.

			fs1 = new FileStream(file1, FileMode.Open);
			fs2 = new FileStream(file2, FileMode.Open);


			if (fs1.Length != fs2.Length)
			{
				// Close the file
				fs1.Close();
				fs2.Close();

				return false;
			}

			do 
			{
				// Read one byte from each file.
				file1byte = fs1.ReadByte();
				file2byte = fs2.ReadByte();
			}
			while ((file1byte == file2byte) && (file1byte != -1));

			// Close the files.
			fs1.Close();
			fs2.Close();



			if ((file1byte - file2byte) == 0)
				return true;
			else
				return false;
		}


		/// <summary>
		/// Sends either comparaison result or coordination message to master.
		/// </summary>
		/// <param name="socket">Socket.</param>
		/// <param name="response">Response.</param>

		private static void SendToMaster(MasterProcess.Command cmd)
		{
			byte[] buffer;
			MemoryStream memoryStream = new MemoryStream ();
			BinaryWriter writer = new BinaryWriter (memoryStream);
			switch (cmd) 
			{
			case MasterProcess.Command.NextFile:

				writer.Write ((int)cmd);
				writer.Write (currentBlockIdx);
				writer.Write (currentFileIdx);
				buffer = memoryStream.ToArray ();

				int sentByte = handler.Send (buffer, 0, buffer.Length, 0);
				if (sentByte == memoryStream.Position) 
				{
					if (verbose) 
					{
						string output = String.Format("Receieved acknowlegdment:\n" +
							"For file block index {0} and file index {1}\n",currentBlockIdx,currentFileIdx);
						Console.WriteLine(output);
						log.WriteLine(output);
					}

					else
					{
						;// send again (not implemented)
					}
				}

				break;

			case MasterProcess.Command.Result:

				writer.Write ((int)cmd);             // writes cmd

				// send context along with results

				writer.Write (currentFileCmpIdx);
				writer.Write (currentFileCmpBlkIdx);
				writer.Write (currentBlkCmpIdx);


				int dim = similarFiles.Count;
				writer.Write ((int)dim);             // writes count of similar files list


				byte[] bytes = null;

				if (dim != 0) {
					// serialize the list of similar file
					bytes = SimilarFilesListSerializer.Serialize (similarFiles);


					writer.Write (bytes.Length);   // size of the byte array

					writer.Write (bytes);
				}


				buffer = memoryStream.ToArray ();

				int sent = 0;
				if ((sent = handler.Send (buffer, 0, buffer.Length, 0)) != 0) {
					Console.WriteLine ("Comparaison result sent to Master\n");
					log.WriteLine ("Comparaison result sent to Master\n");
				}

				sentCmpResTimer.Start();  // start timer to wait for next comparaison
				                          // command from master
				break;

			case MasterProcess.Command.CopyFile:

				writer.Write ((int)cmd);
				buffer = memoryStream.ToArray ();
				handler.Send (buffer, 0, buffer.Length, 0);
				break;
			
			case MasterProcess.Command.Info:

				writer.Write ((int)cmd);
				writer.Write (nbOfComparaisons); // informs Master about the number of
				                                 // files comparaisons done on Slave
				buffer = memoryStream.ToArray ();
				handler.Send (buffer, 0, buffer.Length, 0);

				break;
			}
		}
			
		/// <summary>
		/// Verifies the file hashcode of the received file
		/// </summary>
		/// <returns><c>true</c>, if file hashcode was verifyed, <c>false</c> otherwise.</returns>
		/// <param name="hashCode">Hash code.</param>
		/// <param name="file">File.</param>
		static bool VerifyFileHashcode(string hashCode, string file)
		{
			bool ret = false;
			string fileHash = MasterProcess.GetMD5HashFromFile (file);
			if (fileHash == hashCode)
				ret = true;
			return ret;
		}

		/// <summary>
		/// Raises the timed event event.
		/// </summary>
		/// <param name="source">Source.</param>
		/// <param name="e">E.</param>
		private static void OnTimedEvent(Object source, System.Timers.ElapsedEventArgs e)
		{
			// Send same result to Master on more time

			SendToMaster (nextCmd);
		}

		/// <summary>
		/// Testing similar files generation before serialization.
		/// </summary>
		private static void TestSimilarFiles()
		{
			//StreamWriter logTest = new StreamWriter("TestLog.txt", true);

			lock (logTest) 
			{
				string output = null;

				int count = similarFiles.Count;

				if (count != 0) 
				{
					string.Format ("\n\nComparing file with file Idx {0} and block Idx {1} to all files in block idx {2}\n", currentFileCmpIdx, currentFileCmpBlkIdx, currentBlkCmpIdx);
					logTest.WriteLine (output);

					for (int i = 0; i < count; i++) 
					{
						output = string.Format ("File Idx {0} & Block Idx {1} is similar to File Idx {2} & Block Idx {3}\n",
							similarFiles [i].fileID1.blkfileIdx,
							similarFiles [i].fileID1.blockIdx,
							similarFiles [i].fileID2.blkfileIdx,
							similarFiles [i].fileID2.blockIdx);
						logTest.WriteLine (output);
					}
				}
			}
		}
	}

}

