package com.rao2100.basicspark;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@ConfigurationProperties(prefix = "")
@Configuration
public class AppProperties {
    
    private String appName;
    private String usecase;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getUsecase() {
        return usecase;
    }

    public void setUsecase(String usecase) {
        this.usecase = usecase;
    }
    
    
}
