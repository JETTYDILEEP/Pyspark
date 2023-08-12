import tkinter as tk

class TicTacToe:
    def __init__(self, root):
        self.root = root
        self.root.title("Tic Tac Toe")
        self.root.geometry("525x550")
        self.root.resizable(False, False)

        self.board = [[" " for x in range(3)] for y in range(3)]
        self.player = "X"

        self.buttons = []
        for row in range(3):
            button_row = []
            for col in range(3):
                button = tk.Button(root, text=" ", width=8, height=4, font=("Helvetica", 20), command=lambda row=row, col=col: self.play(row, col))
                button.grid(row=row, column=col)
                button_row.append(button)
            self.buttons.append(button_row)

    def play(self, row, col):
        if self.board[row][col] != " ":
            return

        self.buttons[row][col].configure(text=self.player)
        self.board[row][col] = self.player

        if self.check_win(self.player):
            self.root.title("Player %s wins!" % self.player)
            return

        if self.player == "X":
            self.player = "O"
        else:
            self.player = "X"

    def check_win(self, char):
        # Check rows
        for row in self.board:
            if row == [char, char, char]:
                return True

        # Check columns
        for col in range(3):
            if self.board[0][col] == char and self.board[1][col] == char and self.board[2][col] == char:
                return True

        # Check diagonals
        if self.board[0][0] == char and self.board[1][1] == char and self.board[2][2] == char:
            return True
        if self.board[0][2] == char and self.board[1][1] == char and self.board[2][0] == char:
            return True

        return False

root = tk.Tk()
game = TicTacToe(root)
root.mainloop()
