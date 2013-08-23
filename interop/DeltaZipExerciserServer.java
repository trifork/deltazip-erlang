public class DeltaZipExerciserServer {
    // The following is based on the program in lib/ic/examples/java-client-server/server.java

    public static void main(String[] args) throws java.io.IOException, com.ericsson.otp.erlang.OtpAuthException {
        if (args.length != 2) {
            System.err.println("Usage: java DeltaZipExerciserServer <nodename> <cookie>");
            System.exit(1);
        }
        String snode = args[0];
        String cookie = args[1];
 
        com.ericsson.otp.erlang.OtpServer self = new com.ericsson.otp.erlang.OtpServer(snode, cookie);
 
        System.err.print("Registering with EPMD...");
        boolean res = self.publishPort();
        if (!res) throw new RuntimeException("Node name was already taken.");
        System.err.println("done");
 
        do {
            try {
                com.ericsson.otp.erlang.OtpConnection connection = self.accept();
                System.err.println("Incoming connection.");
                try {
                    handleConnection(connection);
                } catch (Exception e) {
                    System.err.println("Server terminated: "+e);
                } finally {
                    connection.close();
                    System.err.println("Connection terminated.");
                }
            } catch (Exception e) {
                System.err.println("Error accepting connection: "+e);
            }
        } while (true);
    }
 
    static void handleConnection(com.ericsson.otp.erlang.OtpConnection connection) throws Exception {
        while (connection.isConnected() == true) {
            DeltaZipExerciserImpl srv = new DeltaZipExerciserImpl();
            com.ericsson.otp.erlang.OtpInputStream request= connection.receiveBuf();
            try {
                com.ericsson.otp.erlang.OtpOutputStream reply = srv.invoke(request);
                if (reply != null) {
                    connection.sendBuf(srv.__getCallerPid(), reply);
                }
            } catch (Exception e) {
                System.err.println("Server exception: "+e);
                e.printStackTrace(System.err);
                handleException(e, connection, srv.getEnv());
            }
        }
    }
 
    static void handleException(Exception e, com.ericsson.otp.erlang.OtpConnection connection, com.ericsson.otp.ic.Environment env) throws Exception {
        // Write exception reply:
        com.ericsson.otp.erlang.OtpOutputStream err_reply = new com.ericsson.otp.erlang.OtpOutputStream();
        err_reply.write_tuple_head(2);
        err_reply.write_any(env.getSref());
        err_reply.write_tuple_head(2); // Construct return value {error, ErrorText}
        err_reply.write_atom("error");
        err_reply.write_string(e.toString());
        connection.sendBuf(env.getScaller(), err_reply);
    }
 
}
