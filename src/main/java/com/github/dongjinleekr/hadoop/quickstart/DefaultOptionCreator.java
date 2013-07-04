package com.github.dongjinleekr.hadoop.quickstart;

import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;

public class DefaultOptionCreator {
  public static final String DUMMY = "dummy";
  
  public static DefaultOptionBuilder dummyOption() {
    return new DefaultOptionBuilder()
        .withLongName(DUMMY)
        .withRequired(false)
        .withShortName("e")
        .withArgument(
            new ArgumentBuilder().withName(DUMMY).withMinimum(1)
                .withMaximum(1).create())
        .withDescription("dummpy option");
  }
}
