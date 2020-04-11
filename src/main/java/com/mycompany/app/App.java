package com.mycompany.app;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.DataprocScopes;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.JobReference;
import com.google.api.services.dataproc.model.JobStatus;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;

import java.awt.*;
import java.awt.event.ActionListener;
import java.io.*;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

public class App {

    private static JFrame initial;
    private static JFrame result;
    private static JLabel label1, label2;
    private static Storage storage;
    private static File[] files;
    private static final String projectID = "elliptical-rite-273215";
    private static final String bucketName = "dataproc-staging-us-west1-9796963798-pmzp25af";
    private static GoogleCredentials credential;
    private static String curJobId;
    private static DefaultTableModel dataTable;

    public static void main(String[] args) throws IOException {
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                String[] columnNames = { "Term", "Document", "Occurences" };  
                dataTable = new DefaultTableModel(columnNames, 0);

                initial = new JFrame("Cody's Search Engine");
                initial.setSize(750, 750);
                initial.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

                result = new JFrame("Cody's Search Engine");
                result.setSize(750, 750);
                result.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

                welcomeGUI(initial.getContentPane());

                // frame.pack();
                initial.setVisible(true);

                // Setup GCP auth
                try {
                    // Storage
                    files = new File[0];
                    storage = StorageOptions.getDefaultInstance().getService();
                    System.out.println("Authorized storage successfully");

                    // Job Management
                    credential = GoogleCredentials.getApplicationDefault().createScoped(DataprocScopes.all());
                    System.out.println("Authorized job management successfully");

                } catch (Exception e) {
                    System.out.println("Error gathering credentials:");
                    System.out.println(e);
                }

            }
        });
    }

    private static void welcomeGUI(Container pane) {
        ActionListener listener_files = new listener_files();
        ActionListener listener_invertedIndicies = new listener_invertedIndicies();

        pane.setLayout(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();

        JLabel label0 = new JLabel("Load My Engine");
        label0.setFont(label0.getFont().deriveFont(36f));
        pane.add(label0);

        JButton button0 = new JButton("Choose Files");
        button0.addActionListener(listener_files);
        c.fill = GridBagConstraints.HORIZONTAL;
        c.gridx = 0;
        c.gridy = 1;
        c.insets = new Insets(100, 0, 0, 0);
        pane.add(button0, c);

        label1 = new JLabel("<html>");
        label1.setFont(label1.getFont().deriveFont(14f));
        c.fill = GridBagConstraints.HORIZONTAL;
        c.gridx = 0;
        c.gridy = 2;
        c.insets = new Insets(100, 0, 0, 0);
        pane.add(label1, c);

        JButton button1 = new JButton("Construct Inverted Indicies");
        button1.addActionListener(listener_invertedIndicies);
        c.fill = GridBagConstraints.HORIZONTAL;
        c.gridx = 0;
        c.gridy = 3;
        c.insets = new Insets(100, 0, 0, 0);
        pane.add(button1, c);

        label2 = new JLabel();
        label1.setFont(label1.getFont().deriveFont(14f));
        c.fill = GridBagConstraints.HORIZONTAL;
        c.gridx = 0;
        c.gridy = 4;
        c.insets = new Insets(100, 0, 0, 0);
        pane.add(label2, c);
    }

    private static void resultsGUI(Container pane) {
        pane.setLayout(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();

        JLabel label0 = new JLabel("Results:");
        label0.setFont(label0.getFont().deriveFont(36f));
        pane.add(label0);

        JTable j = new JTable(dataTable);
        j.setBounds(0, 0, 500, 650); 
        c.fill = GridBagConstraints.HORIZONTAL;
        c.gridx = 0;
        c.gridy = 1;
        c.insets = new Insets(100, 0, 0, 0);
        pane.add(new JScrollPane(j),c);
    }

    private static void openFileSelector() {
        // Create chooser and setup options
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setMultiSelectionEnabled(true);
        // Show chooser
        int r = fileChooser.showSaveDialog(null);
        // Handle return
        if (r == JFileChooser.APPROVE_OPTION) {
            // Got valid files back
            files = fileChooser.getSelectedFiles();
            for (File file : files) {
                System.out.println(file.getAbsolutePath());
                label1.setText(label1.getText() + "<br>" + file.getName());
            }
            label1.setText(label1.getText() + "</html>");
        } else {
            // Got no files back
            System.out.println("No file selected");
        }
    }

    private static void constructIndices() {
        // Create unique instance ID
        curJobId = UUID.randomUUID().toString();
        // Upload the files to GCP Storage
        uploadFiles();
        // Create the job
        createJob();
        // Join the output from Hadoop
        joinOutput();
        // Display the results from Hadoop 
        displayResults();
    }

    private static void uploadFiles() {
        if (files.length == 0) {
            label2.setText("No files to upload. Moving on to create indexes...");
        }
        for (File file : files) {
            label2.setText("Uploading file: " + file.getName());
            uploadObject(file.getName(), file.getAbsolutePath());
        }
    }

    private static void createJob() {
        label2.setText("Creating job...");
        Job job = null;
        try {
            Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(),
                    new HttpCredentialsAdapter(credential)).setApplicationName("My First Project").build();
            job = dataproc.projects().regions().jobs().submit(projectID, "us-west1",
                    new SubmitJobRequest().setJob(new Job().setReference(new JobReference().setJobId("job-" + curJobId))
                            .setPlacement(new JobPlacement().setClusterName("hadoop-cluster-cmg128"))
                            .setHadoopJob(new HadoopJob().setMainClass("InvertedIndexJob")
                                    .setJarFileUris(ImmutableList.of(
                                            "gs://dataproc-staging-us-west1-9796963798-pmzp25af/JAR/invertedindex.jar"))
                                    .setArgs(ImmutableList.of(
                                            "gs://dataproc-staging-us-west1-9796963798-pmzp25af/Data-" + curJobId,
                                            "gs://dataproc-staging-us-west1-9796963798-pmzp25af/output-" + curJobId)))))
                    .execute();
            label2.setText("Job sent successfully...");
            monitorJob(job, dataproc, curJobId);
        } catch (Exception e) {
            label2.setText("Job not sent successfully");
            System.out.println(e);
        }
    }

    private static void monitorJob(Job job, Dataproc dataproc, String curJobId) {
        Boolean running = true;
        String dot = ".";
        int dots = 1;
        while (running) {
            try {
                job = dataproc.projects().regions().jobs().get(projectID, "us-west1", "job-" + curJobId).execute();
                JobStatus status = job.getStatus();
                if (dots == 4)
                    dots = 1;
                label2.setText("Creating inverted indexes" + dot.repeat(dots++));
                if (status.getState().compareTo("DONE") == 0) {
                    running = false;
                } else if (status.getState().compareTo("ERROR") == 0) {
                    running = false;
                }
            } catch (Exception e) {
                label2.setText("Problem with job");
            }
        }
        label2.setText("Process finished running");
    }

    private static void joinOutput() {
        // Download results (should be multiple)
        for (int i=0; i<8; i++) {
            label2.setText("Downloading output file: part-r-0000" + i);
            downloadObject("output-" + curJobId + "/part-r-0000" + i, "output-" + curJobId + ".txt");
        }
        label2.setText("Finished merging");
    }  

    private static void uploadObject(String objectName, String filePath) {
        // Create BLOB
        BlobId blobId = BlobId.of(bucketName, "Data-" + curJobId + "/" + objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        try {
            storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));
            System.out.println("File " + filePath + " uploaded to bucket " + bucketName + " as " + objectName);
        } catch (Exception e) {
            System.out.println("File " + filePath + " failed to upload to bucket " + bucketName + " as " + objectName);
            System.out.println(e);
        }
    }

    private static void downloadObject(String objectName, String destFilePath) {
        System.out.println("Attempting to download " + objectName + " to " + destFilePath);
        // Create BLOB metadata
        try {
            File file = new File(destFilePath);
            FileWriter fw = new FileWriter(file, true);
            Blob blob = storage.get(BlobId.of(bucketName, objectName));
            ReadChannel readChannel = blob.reader();
            BufferedReader br = new BufferedReader(Channels.newReader(readChannel, "UTF-8"));
            BufferedWriter bw = new BufferedWriter(fw);
            String line = br.readLine();
            while(line != null) {
                // Write to file
                bw.write(line);
                bw.newLine();
                // Add to data array
                String[] parts = line.split("\\s+");
                String term = parts[0];
                String outLine = parts[1];
                String[] outParts = outLine.split("--");
                String doc = outParts[0];
                String freq = outParts[1];
                dataTable.addRow(new String[]{term, doc, freq});
                line = br.readLine();
            }
            bw.flush();
            bw.close();
            System.out.println("Succesful download of " + objectName);
        } catch (Exception e) {
            System.out.println("Failed to download " + objectName + " to " + destFilePath);
            System.out.println(e);
        }
    }

    private static void displayResults() {
        resultsGUI(result.getContentPane());
        initial.setVisible(false);
        result.setVisible(true);
    }

    private static class listener_files implements ActionListener {
        @Override
        public void actionPerformed(java.awt.event.ActionEvent e) {
            System.out.println("Retrieve files");
            openFileSelector();
        }
    }

    private static class listener_invertedIndicies implements ActionListener {
        @Override
        public void actionPerformed(java.awt.event.ActionEvent e) {
            System.out.println("Construct inverted indices");
            new Thread() {
              public void run() {
                constructIndices();
              }
            }.start();
        }
    }    

}