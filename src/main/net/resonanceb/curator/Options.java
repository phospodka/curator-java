package net.resonanceb.curator;

import org.joda.time.LocalDate;

/**
 * Options POJO used to pass to elasticsearch when searching for indices.
 */
public class Options {

    private String prefix;

    private boolean includeOpen;

    private boolean includeClosed;

    private LocalDate cutoff;

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public boolean isIncludeOpen() {
        return includeOpen;
    }

    public void setIncludeOpen(boolean includeOpen) {
        this.includeOpen = includeOpen;
    }

    public boolean isIncludeClosed() {
        return includeClosed;
    }

    public void setIncludeClosed(boolean includeClosed) {
        this.includeClosed = includeClosed;
    }

    public LocalDate getCutoff() {
        return cutoff;
    }

    public void setCutoff(LocalDate cutoff) {
        this.cutoff = cutoff;
    }
}
