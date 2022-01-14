package reduce;

import model.Pair;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.util.*;

import static utils.FileUtil.*;

public class ReduceWorker<K, V> {

    //Declare static variables and constants
    public static final String HOST = "127.0.0.1";
    public static final int PORT = 5555;
    public static final String PATHNAME = "output";

    public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException, InvocationTargetException {

        //Deserialize the input in args
        String dirList = args[0];
        String reducerId = args[1];
        String outputFileDir = args[2];

        //Open socket for communication with master and fetch relevant data
        Socket socket = new Socket(HOST, PORT);
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        String classPath = in.readUTF();

        //dirList to list
        List<String> directories = Arrays.asList(dirList.split(" "));

        //File name
        String fileName = "intermediate_" + reducerId + ".txt";

        List<String> reducerInput = new ArrayList<>();

        // Read file values using static fileUtils
        for (String dir : directories) {
            reducerInput.addAll(readFromIntermediateFile(dir + "/" + fileName));
        }

        //Sort the input
        Collections.sort(reducerInput, (a,b) -> {
            String[] split1 = a.trim().split(" ");
            String[] split2 = b.trim().split(" ");
            return split1[0].compareTo(split2[0]);
        });

        //Convert to Map<K, List<V>>
        Map<String, List<String>> shuffleMap = new HashMap<>();

        //Shuffle
        for (String s : reducerInput) {
            String[] p = s.trim().split(" ");
            if (p.length < 2) continue;

            String key = p[0].trim();
            String value = p[1].trim();
            List<String> tmp = shuffleMap.getOrDefault(key, new ArrayList<>());
            tmp.add(value);
            shuffleMap.put(key, tmp);
        }

        //Read only the specified intermediate
        List<Pair<String, List<String>>> intermediate = new ArrayList<>();
        for (Map.Entry<String, List<String>> e : shuffleMap.entrySet()) {
            intermediate.add(new Pair<>(e.getKey(), e.getValue()));
        }

        //Create output directory and sub-dir
        File dir = new File(PATHNAME+ "/" + outputFileDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }


        Class cls = null;
        try {
            cls = Class.forName(classPath);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        Class partypes[] = new Class[1];
        partypes[0] = Pair.class;
        Method meth = null;
        try {
            meth = cls.getMethod(
                    "reduce", partypes);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }

        //Invoke user defined reduce, cast output to custom Pair class and Hash and write to intermediate files
        List<Pair> pairsList = new ArrayList<>();
        for (Pair<String, List<String>> p : intermediate) {
            //Call user defined reduce function
            Object obj = cls.newInstance();
            Object res = meth.invoke(obj, p);

            Pair<String, String> pair = (Pair<String, String>)res;
            if (pair != null)
                pairsList.add(pair);
        }

        String pairsAsString = pairListToString(pairsList);
        String outFile = PATHNAME+ "/" + outputFileDir + "/" + "output_" + reducerId + ".txt";
        writeToFile(outFile, pairsAsString);

        //Inform master the location of intermediate files
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeUTF(outFile);

        System.out.println("Reduce phase completed successfully for reducer id " + reducerId);
    }
}
