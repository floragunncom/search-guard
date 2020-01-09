package com.floragunn.signals.watch.action.handlers.slack;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

//bad name for this class
//this is the runtime part for email config (that is stored alongside the watch and refers to the destination so that  account = <document id of Destination>)

@JsonIgnoreProperties(ignoreUnknown = true)
public class SlackActionConf {

    private String account;
    private String from;
    private String channel;
    private String text;

    @JsonProperty(value = "icon_emoji")
    private String iconEmoji;

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getIconEmoji() {
        return iconEmoji;
    }

    public void setIconEmoji(String iconEmoji) {
        this.iconEmoji = iconEmoji;
    }

}
