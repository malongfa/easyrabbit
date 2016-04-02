package com.personal.easy.rabbit.consumer;

import com.personal.easy.rabbit.message.Message;

/**
 * Called for every new message. Implementations of this class need to be thread-safe.
 * <p/>
 * This interface does not handle task abortions. Implementations are free to check their Threads interrupted status
 * but are not required to.
 */
public interface MessageCallback{

  /**
   * Called every time a new message is ready to process. Exceptions being thrown are logged but otherwise ignored.
   *
   * @param message to process
   */
  void handleMessage(Message message);
}
