package com.actian.ilabs.dataflow.cassandra.ui.reader;

import com.actian.ilabs.dataflow.cassandra.reader.ReadFromCassandra;
import com.pervasive.datarush.knime.coreui.common.CustomDialogComponent;
import com.pervasive.datarush.ports.PortMetadata;
import org.knime.core.node.InvalidSettingsException;

import javax.swing.*;
import java.awt.*;

/*package*/ final class ReadFromCassandraNodeDialogPane implements CustomDialogComponent<ReadFromCassandra> {

	private final ReadFromCassandraNodeSettings settings = new ReadFromCassandraNodeSettings();
    
	private JTextField nodes;
	private JTextField user;
	private JPasswordField password;
	private JTextArea query;
	
    @Override
    public ReadFromCassandraNodeSettings getSettings() {
        return settings;
    }
    
    @Override
    public boolean isMetadataRequiredForConfiguration(int portIndex) {
        return true;
    }

	@Override
	public Component getComponent() {
		JPanel dialog = new JPanel();
		dialog.setLayout(new BorderLayout());
		
		JPanel authentication = new JPanel();
		authentication.setLayout(new GridLayout(3, 1));
		
		nodes = createTextField("Nodes");
		user = createTextField("User");
		password = createPasswordField("Password");
		
		authentication.add(nodes);
		authentication.add(user);
		authentication.add(password);
		
		dialog.add(authentication, BorderLayout.NORTH);
		
		query = createTextArea();
    	JScrollPane scrollPane = new JScrollPane(query);
    	scrollPane.setBorder(BorderFactory.createTitledBorder("Query"));
    	dialog.add(scrollPane, BorderLayout.CENTER);
    	
    	return dialog;
	}

	@Override
	public void refresh(PortMetadata[] arg0) {
		nodes.setText(settings.nodes.getStringValue());
		user.setText(settings.user.getStringValue());
		password.setText(settings.password.getStringValue());
		query.setText(settings.query.getStringValue());
	}

	@Override
	public void validateAndApplySettings() throws InvalidSettingsException {
		settings.nodes.setStringValue(nodes.getText());
		settings.user.setStringValue(user.getText());
		settings.password.setStringValue(new String(password.getPassword()));
		settings.query.setStringValue(query.getText());
	}
	
	private JPasswordField createPasswordField(String title) {
		JPasswordField passwordField = new JPasswordField();
		passwordField.setBorder(BorderFactory.createTitledBorder(title));
		return passwordField;
	}
	
    private JTextField createTextField(String title) {
        JTextField textField = new JTextField();
        textField.setBorder(BorderFactory.createTitledBorder(title));
        return textField;
    }
    
	private JTextArea createTextArea() {
		JTextArea textArea = new JTextArea();
        textArea.setLineWrap(true);
        textArea.setWrapStyleWord(true);
		return textArea;
	}
}