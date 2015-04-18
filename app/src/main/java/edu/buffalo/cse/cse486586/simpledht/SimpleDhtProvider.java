package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;


public class SimpleDhtProvider extends ContentProvider {

    private static final int SERVER_PORT = 11108;
    private static final String DEBUG = "DEBUG";
    private static final String LOG = "LOG";
    private static final String EXCEPTION = "EXCEPTION";
    private static final String MASTER_AVD = "5554";
    private static final String MASTER_PORT = "11108";
    private static final String SEPARATOR = ":";

    private static final byte MSG_TYPE_NODE_POSITION_REQ = 0;
    private static final byte MSG_TYPE_NODE_POSITION_RESP = 1;

    private String port;
    private String avdPort;
    private String hashedPort;

    private Port successor;
    private Port predecessor;
    private boolean isBoundaryNode;
    private int nodeCount = 1;

    CircularLinkedList chord;

    Lock lock;
    Condition condition;
    boolean isServerCreated;

    Object monitor;
    Object queryMonitor;
    Object networkQueryMonitor;
    int networkQueryReplyCounter;
    MatrixCursor sharedMatrixCursor = null;

    String[] columns = new String[]{"key", "value"};

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        int count = 0;
        Log.d(DEBUG, "Delete request received with selection " + selection);
        try {
            if (selection.equals("\"@\"")) {
                count = deleteLocalContent();
            } else if (selection.equals("\"*\"")) {
                deleteNetworkContent();
                count++;
            } else {
                count = deleteSpecificFile(selection);
                if (count == 0) {
                    Log.d(DEBUG, "Key " + selection + "(" + genHash(selection) + ") not found locally. Routing to " +
                            successor.port + " node.");

                    new Thread(new ClientTask(successor.port,
                            "7" + SEPARATOR + avdPort + SEPARATOR + selection)).start();

                    Log.d(DEBUG, "Created a new thread and routed the request to the successor = " + successor.port);
                }
            }
        } catch (Exception e) {
            Log.d(DEBUG, e.getMessage(), e);
        }
        return count;
    }

    private void deleteNetworkContent() {
        new Thread(new ClientTask(successor.port, "7" + SEPARATOR + avdPort));
    }

    private int deleteSpecificFile(String selection) {
        int count = 0;
        Log.d(DEBUG, "Delete request made with selection = " + selection);

        if (new File(getContext().getFilesDir() + File.pathSeparator + selection).delete()) {
            count++;
        }

        Log.d(DEBUG, "Delete count = " + count);
        return count;
    }

    private int deleteLocalContent() {
        int count = 0;
        Log.d(DEBUG, "Delete request made with @. Deleting all local content");
        Log.d(DEBUG, "Iterating through local files and deleting");

        for (File currentLocalFile : getContext().getFilesDir().listFiles()) {
            boolean delete = currentLocalFile.delete();
            if (delete)
                count++;
        }
        return count;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean onCreate() {
        try {


            Log.d(DEBUG, "Content provider got created!");

            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context
                    .TELEPHONY_SERVICE);
            avdPort = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            port = String.valueOf((Integer.parseInt(avdPort) * 2));

            //TODO handle exception appropriately
            try {
                hashedPort = genHash(avdPort);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            Log.d(DEBUG, "avdPort = " + avdPort);
            Log.d(DEBUG, "port = " + port);

            monitor = new Object();
            //create server thread to handle node joins in case of master
            Thread serverThread = new Thread(new ServerTask());
            serverThread.start();

            try {
                synchronized (monitor) {
                    monitor.wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                successor = new Port(avdPort, genHash(avdPort));
                predecessor = new Port(avdPort, genHash(avdPort));
                isBoundaryNode = true;
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            Log.d(DEBUG, "Checking if this device is master or not");
            if (port.equals(MASTER_PORT)) {
                Log.d(DEBUG, "This is the master node. Creating chord");
                chord = new CircularLinkedList();
            }
            Log.d(DEBUG, "Sending a node join request to the master's server");

            // Type_of_Request : Port :
            String messageToServer = "0" + SEPARATOR + avdPort + "\n";

            Thread clientThread = new Thread(new ClientTask(MASTER_AVD, messageToServer));
            clientThread.start();

        } catch (Exception e) {
            //TODO - remove this after testing
            Log.e(DEBUG, e.getMessage(), e);
        }
        return false;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        try {
            Log.d(DEBUG, "Inserting - " + values.toString());

            String key = values.get("key").toString();
            String value = values.get("value").toString();

            if (predecessor == successor) {
                Log.d(DEBUG, "Successor == Predecessor");

                File file = writeToLocalFile(key, value);
                return Uri.fromFile(file);
            } else {
                try {
                    String hashedKey = genHash(key);

                    Log.d(DEBUG, "hashedKey = " + hashedKey);
                    Log.d(DEBUG, "Is this the boundary node? " + isBoundaryNode);
                    Log.d(DEBUG, "Is hashedKey.compareTo(hashedPort) < 0? " + (hashedKey
                            .compareTo(hashedPort) < 0));
                    Log.d(DEBUG, "hashedKey.compareTo(predecessor.hash) >= 0 " + (hashedKey
                            .compareTo(predecessor.hash) >= 0));
                    Log.d(DEBUG, "hashedPort = " + hashedPort);
                    Log.d(DEBUG, "predecessor.hash" + predecessor.hash);

                    if (isBoundaryNode && (hashedKey.compareTo(hashedPort) < 0 || hashedKey
                            .compareTo(predecessor.hash) >= 0)) {
                        Log.d(DEBUG, "This is the boundary node. Inserting key" + key);
                        File file = writeToLocalFile(key, value);
                        return Uri.fromFile(file);
                    } else if (hashedKey.compareTo(predecessor.hash) > 0 && hashedKey.compareTo
                            (hashedPort) <= 0) {
                        Log.d(DEBUG, "This is the right node. Inserting key" + key);
                        File file = writeToLocalFile(key, value);
                        return Uri.fromFile(file);
                    } else {
                        Log.d(DEBUG, "Out of range while inserting, routing to " + successor.port);
                        new Thread(new ClientTask(successor.port,
                                "2" + SEPARATOR + key + SEPARATOR + value + SEPARATOR +
                                        hashedKey)).start();
                    }
                } catch (NoSuchAlgorithmException e) {
                    Log.e(DEBUG, e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            //TODO - remove this after testing
            Log.e(DEBUG, e.getMessage(), e);
        }
        return null;
    }

    private File writeToLocalFile(String key, String value) {

        File file = new File(getContext().getFilesDir(), key);
        try (
                FileOutputStream outputStream = new FileOutputStream(file);
        ) {
            outputStream.write(value.getBytes());
            Log.d(DEBUG, "Wrote the key-value to the server");

            outputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return file;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        try {
            Log.d(DEBUG, "Querying with selection = " + selection);

            sharedMatrixCursor = null;
            if (selection.equals("\"@\"")) {
                sharedMatrixCursor = queryLocalContent();
            } else if (selection.equals("\"*\"")) {
                sharedMatrixCursor = queryNetworkContent();
            } else {
                sharedMatrixCursor = querySpecificContent(selection);
                if (sharedMatrixCursor == null) {
                    Log.d(DEBUG, "Key " + selection + "(" + genHash(selection) + ") not found locally. Routing to " +
                            successor.port + " node and stopping this thread");
                    try {
                        queryMonitor = new Object();
                        synchronized (queryMonitor) {
                            new Thread(new ClientTask(successor.port,
                                    "3" + SEPARATOR + avdPort + SEPARATOR + selection)).start();
                            queryMonitor.wait();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    Log.d(DEBUG, "Wait is over. Starting this thread again");
                    sharedMatrixCursor.moveToFirst();
                    Log.d(DEBUG, "Cursor count = " + sharedMatrixCursor.getCount());
                    Log.d(DEBUG,
                            "Cursor values = " + sharedMatrixCursor.getString(0) + sharedMatrixCursor.getString(1));

                }
            }
        } catch (Exception e) {
            //TODO - remove this after testing
            Log.e(DEBUG, e.getMessage(), e);
        }
        return sharedMatrixCursor;
    }

    private MatrixCursor queryLocalContent() {
        String[] columns = new String[]{"key", "value"};
        MatrixCursor matrixCursor = new MatrixCursor(columns);

        Log.d(DEBUG, "Begin local search");

        for (File currentLocalFile : getContext().getFilesDir().listFiles()) {
            Log.d(DEBUG, "Retrieving content for local file = " + currentLocalFile.getName());
            try (
                    InputStreamReader inputStreamReader = new InputStreamReader(new
                            FileInputStream(currentLocalFile));
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            ) {

                StringBuilder stringBuilder = new StringBuilder();
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    stringBuilder.append(line);
                }
                matrixCursor.addRow(new Object[]{currentLocalFile.getName(),
                        stringBuilder.toString()});
            } catch (FileNotFoundException e) {
                Log.e(DEBUG, e.getMessage(), e);
            } catch (IOException e) {
                Log.e(DEBUG, e.getMessage(), e);
            }

        }
        return matrixCursor;
    }

    private MatrixCursor queryNetworkContent() {
        sharedMatrixCursor = null;
        networkQueryReplyCounter = 0;

        Log.d(DEBUG, "Network search initiated");
        Log.d(DEBUG, "Successor = " + successor.port);
        Log.d(DEBUG, "Node count = " + nodeCount);

        try {
            networkQueryMonitor = new Object();
            synchronized (networkQueryMonitor) {
                String message = "5" + SEPARATOR + avdPort;
                Log.d(DEBUG, "Sending a network query request to successor. Message = " + message);

                new Thread(new ClientTask(successor.port, message)).start();

                Log.d(DEBUG, "Network query routed to the next node. Waiting this thread");
                networkQueryMonitor.wait();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //Make a matrix cursor here
        return sharedMatrixCursor;
    }

    private MatrixCursor querySpecificContent(String selection) {
        Log.d(DEBUG, "Querying locally for the key = " + selection);
        MatrixCursor matrixCursor = null;

        try (
                InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream
                        (getContext().getFilesDir().getPath() + File.separatorChar + selection));
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        ) {
            matrixCursor = new MatrixCursor(columns);
            StringBuilder stringBuilder = new StringBuilder();
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
            }

            Log.d(DEBUG, "Adding the key-value pair = (" + selection + "-" + stringBuilder
                    .toString() + ")" + " to the cursor");
            matrixCursor.addRow(new Object[]{selection, stringBuilder.toString()});
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return matrixCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    class ServerTask implements Runnable {

        private static final int SERVER_RUNNING_PORT = 10000;

        @Override
        public void run() {
            try {
                Log.d(DEBUG, "Server thread created");
                try (ServerSocket serverSocket = new ServerSocket(SERVER_RUNNING_PORT)) {
                    synchronized (monitor) {
                        monitor.notifyAll();
                    }
                    Socket client = null;
                    while ((client = serverSocket.accept()) != null) {

                        String line = new BufferedReader(new InputStreamReader(client
                                .getInputStream())).readLine();

                        Log.d(DEBUG, "Message from the client = " + line);

                        if ((char) line.charAt(0) == '0') {
                            //Node setup request from client
                            //format will be 0:<port>

                            String clientAvdPort = line.split(SEPARATOR)[1]; //avd of the client
                            Log.d(DEBUG, "Got a node join request from " + clientAvdPort);

                            String hashedClientAvdPort = genHash(clientAvdPort);
                            chord.insert(clientAvdPort, hashedClientAvdPort);

                            Log.d(DEBUG, "Hashed equivalent is " + hashedClientAvdPort);
                            Log.d(DEBUG, "Current chord setup :");

                            Node currentNode = chord.root;

                            try {
                                do {
                                    Log.d(DEBUG, "chord setup = " + currentNode.value.port + " - " +
                                            "" + currentNode.value.hash);
                                    String message = "1" + SEPARATOR + currentNode.value.port +
                                            SEPARATOR + currentNode.next.next.value.port +
                                            SEPARATOR + "X" + SEPARATOR + chord.count + "\n";

                                    if (currentNode.next.equals(chord.root))
                                        message = "1" + SEPARATOR + currentNode.value.port +
                                                SEPARATOR + currentNode.next.next.value.port +
                                                SEPARATOR + "1" + SEPARATOR + chord.count + "\n";

                                    predecessor = new Port(currentNode.value.port,
                                            genHash(currentNode.value.port));
                                    successor = new Port(currentNode.next.next.value.port,
                                            genHash(currentNode.next.next.value.port));
                                    isBoundaryNode = currentNode.next.equals(chord.root) ? true :
                                            false;
                                    Log.d(DEBUG, "Sending neighbor details to port " +
                                            currentNode.next.value.port + " as " + message);
                                    Log.d(DEBUG, message);
                                    new Thread(new ClientTask(currentNode.next.value.port,
                                            message)).start();
                                    currentNode = currentNode.next;
                                } while (currentNode != chord.root);
                            } catch (Exception e) {
                                Log.e(DEBUG, e.getMessage(), e);
                            }
                            Log.d(DEBUG, "Done writing to the client, closing socket");
                            client.close();
                        } else if ((char) line.charAt(0) == '1') {
                            //Node setup reply from master
                            String[] data = line.split(SEPARATOR);

                            String predecessorAvd = data[1];
                            String predecessorHash = genHash(data[1]);

                            String successorAvd = data[2];
                            String successorHash = genHash(data[2]);

                            isBoundaryNode = data[3].equals("1") ? true : false;
                            nodeCount = Integer.parseInt(data[4]);

                            predecessor = new Port(predecessorAvd, predecessorHash);
                            successor = new Port(successorAvd, successorHash);

                            Log.d(DEBUG, "Done receiving neighbor details from server.");
                            Log.d(DEBUG, "Predecessor = " + predecessor.port + "-" + predecessor
                                    .hash);
                            Log.d(DEBUG, "Successor = " + successor.port + "-" + successor.hash);
                            Log.d(DEBUG, "Closing client socket");
                        } else if ((char) line.charAt(0) == '2') {
                            //Insert into node
                            String[] data = line.split(SEPARATOR);
                            String key = data[1];
                            String value = data[2];
                            String hashedKey = data[3];

                            Log.d(DEBUG, "My range - " + predecessor.hash + " to " + successor
                                    .hash);
                            Log.d(DEBUG, "key when hashed = " + hashedKey);

                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht" +
                                    ".provider");
                            ContentValues cv = new ContentValues();
                            cv.put("key", key);
                            cv.put("value", value);

                            Log.d(DEBUG, "ContentValues = " + cv);
                            Log.d(DEBUG, "Running insert code");
                            insert(mUri, cv);

                        } else if ((char) line.charAt(0) == '3') {
                            //query and reply to this node
                            String[] data = line.split(SEPARATOR);
                            String queryingPort = data[1];
                            String selection = data[2];

                            MatrixCursor cursor = querySpecificContent(selection);

                            if (cursor == null) {
                                //forward to next port
                                new Thread(new ClientTask(successor.port,
                                        "3" + SEPARATOR + queryingPort + SEPARATOR + selection))
                                        .start();
                            } else {
                                //reply back to the calling thread
                                cursor.moveToFirst();
                                new Thread(new ClientTask(queryingPort,
                                        "4" + SEPARATOR + avdPort + SEPARATOR + cursor.getString(0) + SEPARATOR +
                                                cursor.getString(1))).start();
                            }

                        } else if ((char) line.charAt(0) == '4') {
                            //query response
                            synchronized (queryMonitor) {
                                String[] data = line.split(SEPARATOR);
                                String replyingPort = data[1];
                                String selection = data[2];
                                String response = data[3];

                                sharedMatrixCursor = new MatrixCursor(columns);
                                Log.d(DEBUG,
                                        "Adding the selection received from " + replyingPort + " to the sharedCursor");
                                Log.d(DEBUG, "selection = " + selection + ". response = " + response);
                                sharedMatrixCursor.addRow(new Object[]{selection, response});
                                queryMonitor.notifyAll();
                            }

                        } else if ((char) line.charAt(0) == '5') {
                            //network search
                            String[] data = line.split(SEPARATOR);
                            String requestingPort = data[1];

                            MatrixCursor localSearchCursor = queryLocalContent();
                            localSearchCursor.moveToFirst();
                            StringBuilder stringBuilder = new StringBuilder("6" + SEPARATOR + avdPort + SEPARATOR);
                            while (!localSearchCursor.isAfterLast()) {
                                stringBuilder.append(localSearchCursor.getString(0) + SEPARATOR +
                                        localSearchCursor.getString(1) + SEPARATOR);
                                localSearchCursor.moveToNext();
                            }
                            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                            new Thread(new ClientTask(requestingPort, stringBuilder.toString())).start();

                            if (!requestingPort.equals(avdPort)) {
                                //Don't forward if this is the requesting port.
                                new Thread(new ClientTask(successor.port, line)).start();
                            }

                        } else if ((char) line.charAt(0) == '6') {
                            //network search
                            //collate the data and finish MatrixCursor. notify the lock once done.
                            String[] data = line.split(SEPARATOR);
                            String replyingPort = data[1];
                            networkQueryReplyCounter++;
                            if (sharedMatrixCursor == null) {
                                sharedMatrixCursor = new MatrixCursor(columns);
                            }

                            for (int i = 2; i < data.length; i += 2) {
                                sharedMatrixCursor.addRow(new Object[]{data[i], data[i + 1]});
                            }

                            if (networkQueryReplyCounter == nodeCount) {
                                synchronized (networkQueryMonitor) {
                                    networkQueryMonitor.notifyAll();
                                }
                            }
                        } else if ((char) line.charAt(0) == '7') {
                            String[] data = line.split(SEPARATOR);
                            String requestingPort = data[1];
                            String selection = data[2];
                            Log.d(DEBUG, "Deleting local files");
                            int deleteCount = deleteLocalContent();

                            if (!requestingPort.equals(port)) {
                                //forward to the next node
                                Log.d(DEBUG, "Local files deleted. Forwarding the request to the next node");
                                new Thread(new ClientTask(successor.port, line));
                            }else{
                                Log.d(DEBUG, "Loop complete. Done deleting");
                            }
                        }
                    }
                } catch (IOException e) {
                    Log.e(DEBUG, e.getMessage(), e);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                //TODO - remove this after testing
                Log.e(DEBUG, e.getMessage(), e);
            }
        }


    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    class ClientTask implements Runnable {

        String portToWrite;
        ByteBuffer byteBuffer;
        String messageToServer;

        ClientTask(String portToWrite, ByteBuffer byteBuffer) {
            this.portToWrite = portToWrite;
            this.byteBuffer = byteBuffer;
        }

        ClientTask(String portToWrite, String messageToServer) {
            this.portToWrite = portToWrite;
            this.messageToServer = messageToServer;
        }

        @Override
        public void run() {
            try {
                Log.d(DEBUG, "Created a client thread.");

                try (Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(portToWrite) * 2);
                     OutputStream os = socket.getOutputStream();) {

                    Log.d(DEBUG, "Sending the message to server :" + messageToServer);
                    os.write(messageToServer.getBytes());
                    os.flush();
                    Log.d(DEBUG, "Message flushed from client");

                    socket.close();
                } catch (IOException e) {
                    Log.e(DEBUG, e.getMessage(), e);
                }
            } catch (Exception e) {
                //TODO - remove this after testing
                Log.e(DEBUG, e.getMessage(), e);
            }

        }
    }
}