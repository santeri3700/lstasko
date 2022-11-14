#
# Copyright (C) 2022 Santeri Pikarinen <santeri3700>
#
# This file is part of lstasko.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License version 2 for more details.
#
# You should have received a copy of the GNU General Public License version 2
# along with this program; if not, see <http://www.gnu.org/licenses/>.

class LSTaskoException(Exception):
    """ LSTasko Generic Exception """


class LSTaskoNoRhnConfException(Exception):
    """ LSTasko can't open or read rhn.conf """


class LSTaskoDatabaseNotConnectedException(Exception):
    """ LSTasko not connected to database """
    def __str__(self):
        return "Not connected to a database!"


class LSTaskoChannelNotFoundException(Exception):
    """ LSTAsko couldn't find a channel """
    def __init__(self, missing, *args):
        super().__init__(args)
        self.missing = missing

    def __str__(self):
        return f"Coundn't find channels with the following identifiers: {self.missing}"
