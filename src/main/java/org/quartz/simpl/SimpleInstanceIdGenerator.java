package org.quartz.simpl;

import java.util.UUID;
import org.quartz.SchedulerException;
import org.quartz.spi.InstanceIdGenerator;

/**
 * Default generator for a node's lease owner id. Each process start gets a new UUID so identity is
 * not recycled across restarts.
 *
 * @see InstanceIdGenerator
 */
public class SimpleInstanceIdGenerator implements InstanceIdGenerator {
  public String generateInstanceId() throws SchedulerException {
    return UUID.randomUUID().toString();
  }
}
