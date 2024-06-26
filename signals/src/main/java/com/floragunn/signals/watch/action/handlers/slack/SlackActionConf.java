package com.floragunn.signals.watch.action.handlers.slack;

import java.util.List;
import java.util.Map;

import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.OrderedImmutableMap;

//bad name for this class
//this is the runtime part for email config (that is stored alongside the watch and refers to the destination so that  account = <document id of Destination>)

public class SlackActionConf implements Document<SlackActionConf> {

    private String account;
    private String from;
    private String channel;
    private String text;
    private List<Map<String, ?>> blocks;
    private List<Map<String, ?>> attachments;
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

    public List<Map<String, ?>> getBlocks() {
        return blocks;
    }

    public void setBlocks(List<Map<String, ?>> blocks) {
        this.blocks = blocks;
    }

    public List<Map<String, ?>> getAttachments() {
        return attachments;
    }

    public void setAttachments(List<Map<String, ?>> attachments) {
        this.attachments = attachments;
    }

    @Override
    public Object toBasicObject() {
        OrderedImmutableMap<String, Object> result = OrderedImmutableMap.ofNonNull("account", account, "from", from, "channel", channel, "text", text, "icon_emoji", iconEmoji);

        if (blocks != null && !blocks.isEmpty()) {
            result = result.with("blocks", blocks);
        }
        
        if (attachments != null && !attachments.isEmpty()) {
            result = result.with("attachments", attachments);
        }
        
        return result;
    }
}
