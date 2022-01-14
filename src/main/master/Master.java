package master;

import map.MapWorker;
import model.Offsets;
import reduce.ReduceWorker;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static utils.FileUtil.*;

public class Master {
    public static final int MAPPER_COMM_PORT = 6666;
    public static final int REDUCER_COMM_PORT = 5555;

    List<String> intermediateDir = new ArrayList<>();
    List<String> outputFiles = new ArrayList<>();

    ServerSocket mapperServer;
    DataOutputStream mapperOut;
    DataInputStream mapperIn;
    Socket mapperConnection;
    ServerSocket reducerServer;
    DataOutputStream reducerOut;
    DataInputStream reducerIn;
    Socket reducerConnection;

    private final AtomicBoolean mapperThreadExit = new AtomicBoolean(false);
    private final AtomicBoolean reducerThreadExit = new AtomicBoolean(false);

    //Primary method of the class that performs Map Reduce based on properties supplied
    public void mapReduce(Properties properties) throws IOException, InterruptedException {
        System.out.println("Starting mapReduce in Master");

        //Get number of mappers/reducers, input file, mapReduce UDF class from config
        int N = Integer.parseInt(properties.getProperty("N"));
        String inputFile = properties.getProperty("inputFilePath");
        String udfClass = properties.getProperty("udfClass");
        String removePunctuations = properties.getProperty("removePunctuations");
        String outputFileDir = properties.getProperty("outputFileDir");
        Boolean induceFault = Boolean.parseBoolean(String.valueOf(properties.getOrDefault("induceFault", "FALSE")));

        //Start thread for mapper-master socket communication
        startThreadForSocketMapper(inputFile, N, udfClass);

        //Partition the filecontents by number of mappers
        //Get offsets for each mapper
        List<Offsets> offsets = getMapperOffsets(inputFile, N);

        // Spawn n mapper processes
        List<Process> mapProcesses = new ArrayList<>();

        //Call map function along with required args
        for (int i = 0; i < N; i++) {
            List<String> mainArgs = new ArrayList<>();
            mainArgs.add(String.valueOf(offsets.get(i).getOffsetStart()));
            mainArgs.add(String.valueOf(offsets.get(i).getLength()));
            mainArgs.add(String.valueOf(i + 1));
            mainArgs.add(removePunctuations);

            mapProcesses.add(exec(MapWorker.class, mainArgs).start());
        }

        //Induce fault if specified (for testing)
        if (induceFault) {
            System.out.println("Inducing fault to test fault tolerance");
            mapProcesses.get(0).destroy();
        }

        //Wait for all mappers to finish
        for (int i = 0; i < N; i++) {
            mapProcesses.get(i).waitFor();
        }

        //Fault tolerance for one mapper failure
        int i = 0;
        for (; i < N; i++) {
            if (mapProcesses.get(i).exitValue() != 0) {
                System.out.println("Map worker failure. Respawning one map worker");
                List<String> mainArgs = new ArrayList<>();
                mainArgs.add(String.valueOf(offsets.get(i).getOffsetStart()));
                mainArgs.add(String.valueOf(offsets.get(i).getLength()));
                mainArgs.add(String.valueOf(i + 1));
                mainArgs.add(removePunctuations);

                mapProcesses.add(i, exec(MapWorker.class, mainArgs).start());
                break;
            }
        }
        if (i < N)
            mapProcesses.get(i).waitFor();

        //Convert intermediate dir list to string
        String dirList = String.join(" ", intermediateDir);

        //Reducer phase
        //Start socket for communication
        startThreadForSocketReducer(udfClass, N);

        //Spawn n reducer processes
        List<Process> reduceProcesses = new ArrayList<>();

        //Call reduce function
        //Return location of output files
        for (int j = 0; j < N; j++) {
            List<String> mainArgs = new ArrayList<>();
            mainArgs.add(dirList);
            mainArgs.add(String.valueOf(j + 1));
            mainArgs.add(outputFileDir);

            reduceProcesses.add(exec(ReduceWorker.class, mainArgs).start());
        }

        for (int j = 0; j < N; j++) {
            reduceProcesses.get(j).waitFor();
        }

        //Fault tolerance for one reducer failure
        int j = 0;
        for (; j < N; j++) {
            if (reduceProcesses.get(j).exitValue() != 0) {
                System.out.println("Reduce worker failure. Respawning one reduce worker");
                List<String> mainArgs = new ArrayList<>();
                mainArgs.add(dirList);
                mainArgs.add(String.valueOf(j + 1));
                mainArgs.add(outputFileDir);

                reduceProcesses.add(j, exec(ReduceWorker.class, mainArgs).start());
                break;
            }
        }
        if (j < N)
            reduceProcesses.get(j).waitFor();

        System.out.println("Reducer phase complete");
        System.out.println("---------------------------------------------------------------");
        System.out.println("Output files: ");
        outputFiles.forEach(System.out::println);

        //End socket communication
        endSocketCommunication();

    }

    //Function to start socket communication between mapper-master
    public void startThreadForSocketMapper(String inputFile, Integer N, String classPath) throws IOException {
        mapperServer = new ServerSocket(MAPPER_COMM_PORT);
        mapperThreadExit.set(true);
        Thread serverThread = new Thread(() -> {
            while (mapperThreadExit.get()) {
                try {
                    mapperConnection = mapperServer.accept();
                    mapperOut = new DataOutputStream(mapperConnection.getOutputStream());
                    mapperIn = new DataInputStream(
                            new BufferedInputStream(mapperConnection.getInputStream()));

                    //Send values to map workers
                    mapperOut.writeUTF(inputFile);
                    mapperOut.writeUTF(classPath);
                    mapperOut.writeUTF(String.valueOf(N));

                    //Receive location of intermediate files from map workers
                    intermediateDir.add(mapperIn.readUTF());

                    if (intermediateDir.size() == N) {
                        shutMapperThread();
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Throwable t) {
                    t.printStackTrace();
                    throw t;
                }
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();
    }

    //Function to start socket communication between reducer-master
    public void startThreadForSocketReducer(String classPath, Integer N) throws IOException {
        reducerServer = new ServerSocket(REDUCER_COMM_PORT);
        reducerThreadExit.set(true);
        Thread serverThread = new Thread(() -> {
            while (reducerThreadExit.get()) {
                try {
                    reducerConnection = reducerServer.accept();
                    reducerOut = new DataOutputStream(reducerConnection.getOutputStream());
                    reducerIn = new DataInputStream(
                            new BufferedInputStream(reducerConnection.getInputStream()));

                    //Send values to reduce workers
                    reducerOut.writeUTF(classPath);

                    //Receive location of output files from map workers
                    outputFiles.add(reducerIn.readUTF());

                    if (outputFiles.size() == N) {
                        shutReducerThread();
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Throwable t) {
                    t.printStackTrace();
                    throw t;
                }
            }
        });

        serverThread.setDaemon(true);
        serverThread.start();
    }

    //Method to create sub processes for map and reduce workers
    public ProcessBuilder exec(Class clazz, List<String> mainArgs) {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = clazz.getName();

        List<String> command = new ArrayList<>();
        command.add(javaBin);
        command.add("-cp");
        command.add(classpath);
        command.add(className);
        command.addAll(mainArgs);

        ProcessBuilder builder = new ProcessBuilder(command);
        return builder;
    }

    public void shutMapperThread() {
        mapperThreadExit.set(false);
    }

    public void shutReducerThread() {
        reducerThreadExit.set(false);
    }

    public void endSocketCommunication() {
        try {
            mapperConnection.close();
            reducerConnection.close();
            mapperIn.close();
            reducerIn.close();
            mapperOut.close();
            reducerOut.close();
            mapperServer.close();
            reducerServer.close();
            shutMapperThread();
            shutReducerThread();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
