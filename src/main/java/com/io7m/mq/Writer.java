/*
 * Copyright © 2021 Mark Raynsford <code@io7m.com> https://www.io7m.com
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR
 * IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package com.io7m.mq;

import com.io7m.jmulticlose.core.CloseableCollection;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;

import java.time.Duration;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class Writer
{
  private Writer()
  {

  }

  public static void main(
    final String[] args)
    throws Exception
  {
    try (var resources = CloseableCollection.create()) {
      final var locator =
        resources.add(ActiveMQClient.createServerLocator(
          "tcp://messaging.int.arc7.info:61000?sslEnabled=true"));

      final var sessions =
        locator.createSessionFactory();
      final var session =
        resources.add(
          sessions.createSession(true, true));
      final var producer =
        resources.add(
          session.createProducer("info.arc7.unrestricted.base"));

      int index = 0;
      while (true) {
        final var text =
          String.format("Hello %d", Integer.valueOf(index));
        final var expiration =
          Duration.ofHours(1L).toMillis();

        final var message = session.createMessage(false);
        message.setExpiration(expiration);
        message.writeBodyBufferBytes(text.getBytes(UTF_8));
        producer.send(message);

        ++index;

        try {
          Thread.sleep(1_000L);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
