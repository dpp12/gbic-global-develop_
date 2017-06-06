package com.telefonica.gbic.global;

import java.text.MessageFormat;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class GbicGPlatformConfig {
    
    private static final String BUNDLE_NAME = "com.telefonica.gbic.global.gbic-gplatform-config"; //$NON-NLS-1$
    
    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME);
    
    /* Private constructor to avoid instanciation */
    private GbicGPlatformConfig() {}
    
    /**
     * Get the value of the property given as argument
     * @param propName The name of the property
     * @return Value of the property
     */
    public static String getValue(String propName) {
        try {
            return RESOURCE_BUNDLE.getString(propName);
        } catch (MissingResourceException e) {
            return '!' + propName + '!';
        }
    }
    
    /**
     * Get the value of the property given as first argument
     * @param propName The name of the property
     * @param args Parameters for the property
     * @return Value of the property
     */
    public static String getValue(String propName, String ... args) {
        try {
            return MessageFormat.format(RESOURCE_BUNDLE.getString(propName), (Object [])args);
        } catch (MissingResourceException e) {
            return '!' + propName + '!';
        }
    }
}
