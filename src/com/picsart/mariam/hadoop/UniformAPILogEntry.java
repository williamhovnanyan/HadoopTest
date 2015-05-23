package com.picsart.mariam.hadoop;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by mariam on 5/23/15.
 */
public class UniformAPILogEntry  implements Writable {
    private String reqType, hostName, ip, requestString, applicationInfo,
            deviceId, countryCode, languageCode, platform;
    private Integer statusCode, responseSizeInBytes, appVersion;
    private Long requestTime;
    private Float processingTime;


    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public void setRequestString(String requestString) {
        this.requestString = requestString;
    }

    public void setApplicationInfo(String applicationInfo) {
        this.applicationInfo = applicationInfo;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public void setLanguageCode(String languageCode) {
        this.languageCode = languageCode;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public void setStatusCode(Integer statusCode) {
        this.statusCode = statusCode;
    }

    public void setResponseSizeInBytes(Integer responseSizeInBytes) {
        this.responseSizeInBytes = responseSizeInBytes;
    }

    public void setAppVersion(Integer appVersion) {
        this.appVersion = appVersion;
    }

    public void setRequestTime(Long requestTime) {
        this.requestTime = requestTime;
    }

    public void setProcessingTime(Float processingTime) {
        this.processingTime = processingTime;
    }

    public String getReqType() {
        return reqType;
    }

    public String getHostName() {

        return hostName;
    }

    public String getIp() {
        return ip;
    }

    public String getRequestString() {
        return requestString;
    }

    public String getRequestURL() {
        if(requestString != null) {
            return requestString.substring(0, requestString.indexOf('?') != -1
                    ? requestString.indexOf('?') : requestString.length());
        }
        return null;
    }

    public String getApplicationInfo() {
        return applicationInfo;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public String getLanguageCode() {
        return languageCode;
    }

    public String getPlatform() {
        return platform;
    }

    public Integer getResponseSizeInBytes() {
        return responseSizeInBytes;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public Integer getAppVersion() {
        return appVersion;
    }

    public Long getRequestTime() {
        return requestTime;
    }

    public Float getProcessingTime() {
        return processingTime;
    }

    public UniformAPILogEntry(String reqType) {
        this.reqType = reqType;
        if(this.reqType == null)
            throw new NullPointerException("Request type cannot be null");
    }


    public void write(DataOutput dataOutput) throws IOException{
        dataOutput.writeInt(reqType.length());
        dataOutput.writeBytes(getReqType());

        dataOutput.writeBoolean(getHostName() == null);
        if(getHostName() != null) {
            dataOutput.writeInt(getHostName().length());
            dataOutput.writeBytes(getHostName());
        }

        dataOutput.writeBoolean(getIp() == null);
        if(getIp() != null) {
            dataOutput.writeInt(getIp().length());
            dataOutput.writeBytes(getIp());
        }

        dataOutput.writeBoolean(getRequestString() == null);
        if(getRequestString() != null) {
            dataOutput.writeInt(getRequestString().length());
            dataOutput.writeBytes(getRequestString());
        }

        dataOutput.writeBoolean(getApplicationInfo() == null);
        if(getApplicationInfo() != null) {
            dataOutput.writeInt(getApplicationInfo().length());
            dataOutput.writeBytes(getApplicationInfo());
        }

        dataOutput.writeBoolean(getDeviceId() == null);
        if(getDeviceId() != null) {
            dataOutput.writeInt(getDeviceId().length());
            dataOutput.writeBytes(getDeviceId());
        }

        dataOutput.writeBoolean(getCountryCode() == null);
        if(getCountryCode() != null) {
            dataOutput.writeInt(getCountryCode().length());
            dataOutput.writeBytes(getCountryCode());
        }

        dataOutput.writeBoolean(getLanguageCode() == null);
        if(getLanguageCode() != null) {
            dataOutput.writeInt(getLanguageCode().length());
            dataOutput.writeBytes(getLanguageCode());
        }

        dataOutput.writeBoolean(getPlatform() == null);
        if(getPlatform() != null) {
            dataOutput.writeInt(getPlatform().length());
            dataOutput.writeBytes(getPlatform());
        }

        dataOutput.writeBoolean(getStatusCode() == null);
        if(getStatusCode() != null) {
            dataOutput.writeInt(getStatusCode());
        }

        dataOutput.writeBoolean(getResponseSizeInBytes() == null);
        if(getResponseSizeInBytes() != null) {
            dataOutput.writeInt(getResponseSizeInBytes());
        }

        dataOutput.writeBoolean(getAppVersion() == null);
        if(getAppVersion() != null) {
            dataOutput.writeInt(getAppVersion());
        }

        dataOutput.writeBoolean(getRequestTime() == null);
        if(getRequestTime() != null) {
            dataOutput.writeLong(getRequestTime());
        }

        dataOutput.writeBoolean(getProcessingTime() == null);
        if(getProcessingTime() != null) {
            dataOutput.writeFloat(getProcessingTime());
        }

    }

    public void readFields(DataInput dataInput) throws IOException {
        int length = dataInput.readInt();
        byte[] inputStreamHolder = new byte[length];

        dataInput.readFully(inputStreamHolder);
        this.reqType = new String(inputStreamHolder, "UTF-8");

        if(!dataInput.readBoolean()) {
            length = dataInput.readInt();
            inputStreamHolder = new byte[length];

            dataInput.readFully(inputStreamHolder);
            setHostName(new String(inputStreamHolder, "UTF-8"));
        }

        if(!dataInput.readBoolean()) {
            length = dataInput.readInt();
            inputStreamHolder = new byte[length];

            dataInput.readFully(inputStreamHolder);
            setIp(new String(inputStreamHolder, "UTF-8"));
        }

        if(!dataInput.readBoolean()) {
            length = dataInput.readInt();
            inputStreamHolder = new byte[length];

            dataInput.readFully(inputStreamHolder);
            setRequestString(new String(inputStreamHolder, "UTF-8"));
        }

        if(!dataInput.readBoolean()) {
            length = dataInput.readInt();
            inputStreamHolder = new byte[length];

            dataInput.readFully(inputStreamHolder);
            setApplicationInfo(new String(inputStreamHolder, "UTF-8"));
        }

        if(!dataInput.readBoolean()) {
            length = dataInput.readInt();
            inputStreamHolder = new byte[length];

            dataInput.readFully(inputStreamHolder);
            setDeviceId(new String(inputStreamHolder, "UTF-8"));
        }

        if(!dataInput.readBoolean()) {
            length = dataInput.readInt();
            inputStreamHolder = new byte[length];

            dataInput.readFully(inputStreamHolder);
            setCountryCode(new String(inputStreamHolder, "UTF-8"));
        }

        if(!dataInput.readBoolean()) {
            length = dataInput.readInt();
            inputStreamHolder = new byte[length];

            dataInput.readFully(inputStreamHolder);
            setLanguageCode(new String(inputStreamHolder, "UTF-8"));
        }

        if(!dataInput.readBoolean()) {
            length = dataInput.readInt();
            inputStreamHolder = new byte[length];

            dataInput.readFully(inputStreamHolder);
            setPlatform(new String(inputStreamHolder, "UTF-8"));
        }

        if(!dataInput.readBoolean()) {
            setStatusCode(dataInput.readInt());
        }

        if(!dataInput.readBoolean()) {
            setResponseSizeInBytes(dataInput.readInt());
        }

        if(!dataInput.readBoolean()) {
            setAppVersion(dataInput.readInt());
        }

        if(!dataInput.readBoolean()) {
            setRequestTime(dataInput.readLong());
        }

        if(!dataInput.readBoolean()) {
            setProcessingTime(dataInput.readFloat());
        }
    }
}
