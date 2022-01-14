package map;

import model.Offsets;
import model.Pair;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.net.Socket;
import java.util.List;

import static utils.FileUtil.*;

import java.lang.reflect.*;

//Map Worker class - Mapper processes created run this class
public class MapWorker<K, V> {

    //Declare static variables and path
    public static final String HOST = "127.0.0.1";
    public static final int PORT = 6666;
    private static final String PATHNAME ="intermediate" ;

    public static void main(String[] args ) throws Exception {

        //Deserialize the input in args
        int offset = Integer.parseInt(args[0]);
        int length = Integer.parseInt(args[1]);
        int mapperId = Integer.parseInt(args[2]);
        boolean removePunctuations = Boolean.parseBoolean(args[3]);

//        if(mapperId == 2)
//            System.exit(130);

        //Open socket for communication with master and fetch relevant data
        Socket socket = new Socket(HOST, PORT);
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        String inputFile = in.readUTF();
        String classPath = in.readUTF();
        int N = Integer.parseInt(in.readUTF());

        //Create output directory
        File dir = new File(PATHNAME);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        //Read only the specified input
        String input = readWithOffset(inputFile, new Offsets(offset, length), removePunctuations);
        Pair<String,Integer>  mapperInput = stringToPair(input);

        //Generate the directories for hashed map output to go into
        String dirPath = generateIntermediateDirectoryStruc(mapperId, N);

        //Use java reflections to run map function from user defined class ( UDF )
        Class cls = Class.forName(classPath);
        Class partypes[] = new Class[1];
        partypes[0] = Pair.class;
        Method meth = cls.getMethod("map", partypes);

        // Get object from .class file
        Object obj = cls.newInstance();

        //Invoke user defined map , cast output to custom Pair class and Hash and write to intermediate files
        Object res = meth.invoke(obj, mapperInput);
        List<Pair> list = (List<Pair>) res;
        for(Pair p: list) {
            writeToIntFile(mapperId, p, N);
        }

        //Inform master the location of intermediate files
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeUTF(dirPath);

       System.out.println("Map phase completed successfully for mapper id " + mapperId);

        System.exit(0);
    }


}
