/*
 * Copyright (c) 2015, Mazen Kotb <email@mazenmc.io>
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
package io.minecloud.cli.handler;

import asg.cliche.Shell;
import asg.cliche.ShellFactory;
import io.minecloud.MineCloudException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public abstract class AbstractHandler {
    private Shell current;

    protected AbstractHandler() {
    }

    public AbstractHandler(Shell shell) {
        this.current = shell;
    }

    public void enterShell(AbstractHandler subHandler, String name) {
        try {
            subHandler.current = ShellFactory.createSubshell(name, current, "minecloud", subHandler);
            subHandler.current.commandLoop();
        } catch (IOException ex) {
            throw new MineCloudException("Error encountered when in sub-command loop!", ex);
        }
    }

    public String optionPrompt(String... optins) {
        List<String> options = Arrays.asList(optins);

        System.out.println("Available options: (enter number)");

        for (int i = 0; i < options.size(); i++) {
            System.out.println(i + ": " + options.get(i));
        }

        int option = new Scanner(System.in).nextInt();

        if (option < 0 || option >= options.size()) {
            System.out.println("\nInvalid option! Trying again...\n\n");

            optionPrompt(optins);
        }

        return options.get(option);
    }

    public Shell currentShell() {
        return current;
    }
}