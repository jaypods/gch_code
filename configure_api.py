"""User-operated native window. Never exposes secrets to a web endpoint."""
from pathlib import Path
import tkinter as tk
from tkinter import messagebox
from credential_store import WindowsCredentialStore, CredentialError, validate_pair, validation_summary

HERE = Path(__file__).resolve().parent


def main():
    root = tk.Tk()
    root.title('C801 · Local API Configuration')
    root.geometry('640x610')
    root.resizable(False, False)
    root.configure(bg='#101a2b')
    root.iconbitmap(str(HERE / 'assets/c801.ico'))
    root.option_add('*Font', ('Segoe UI', 10))
    root.option_add('*Entry.show', '●')
    outer = tk.Frame(root, bg='#101a2b', padx=30, pady=24)
    outer.pack(fill='both', expand=True)
    header = tk.Frame(outer, bg='#101a2b'); header.pack(fill='x')
    logo = tk.PhotoImage(file=str(HERE / 'assets/c801.png')).subsample(8, 8)
    tk.Label(header, image=logo, bg='#101a2b').pack(side='left', padx=(0, 14))
    titles = tk.Frame(header, bg='#101a2b'); titles.pack(side='left')
    tk.Label(titles, text='Configure API credentials locally', fg='#ffe3a0', bg='#101a2b',
             font=('Segoe UI', 19, 'bold')).pack(anchor='w')
    tk.Label(titles, text='Local storage only · No test orders · Trading stays disabled', fg='#a9bbd0', bg='#101a2b').pack(anchor='w', pady=(6, 0))
    tk.Label(outer, text='For Binance system-generated HMAC keys.\nUse credentials that have never been shared in a chat or repository.',
             justify='left', anchor='w', fg='#b8c6d9', bg='#101a2b').pack(fill='x', pady=(20, 12))
    fields = []
    for label in ('API Key', 'Secret Key'):
        tk.Label(outer, text=label, fg='#e6edf7', bg='#101a2b', anchor='w').pack(fill='x', pady=(10, 5))
        entry = tk.Entry(outer, show='●', bg='#1c2a40', fg='#eef4ff', insertbackground='#eef4ff',
                         relief='flat', font=('Segoe UI', 13), exportselection=False)
        entry.pack(fill='x', ipady=8)
        fields.append(entry)
    confirmed = tk.BooleanVar(value=False)
    tk.Checkbutton(outer, text='I own these credentials and withdrawals are disabled', variable=confirmed,
                   bg='#101a2b', fg='#dce7f5', selectcolor='#233550', activebackground='#101a2b',
                   activeforeground='#ffffff', anchor='w').pack(fill='x', pady=(18, 10))
    info = tk.StringVar(value='Fields are checked locally. Formatting whitespace is removed when saving.')
    tk.Label(outer, textvariable=info, fg='#8cdbc2', bg='#101a2b', wraplength=570,
             anchor='w', justify='left').pack(fill='x', pady=(2, 12))

    def clear():
        for field in fields:
            field.delete(0, 'end')

    def describe(event=None):
        info.set(validation_summary(fields[0].get(), fields[1].get()))

    for field in fields:
        field.bind('<KeyRelease>', describe)
        field.bind('<<Paste>>', lambda event: root.after_idle(describe), add='+')
        field.bind('<<Cut>>', lambda event: root.after_idle(describe), add='+')

    def save():
        if not confirmed.get():
            messagebox.showinfo('Not saved', 'First confirm credential ownership and that withdrawals are disabled.', parent=root)
            return
        try:
            key, secret = validate_pair(fields[0].get(), fields[1].get())
            store = WindowsCredentialStore()
            if store.metadata()['configured'] and not messagebox.askyesno(
                    'Replace local credentials', 'Credentials already exist for this Windows user. Replace them? Other C801 installations share this entry. This does not enable trading.', parent=root):
                return
            store.save(key, secret)
            clear()
            confirmed.set(False)
            info.set('Saved in Windows Credential Manager. Exchange permissions have not been verified.')
        except CredentialError as error:
            messagebox.showerror('Not saved', str(error), parent=root)
        except Exception:
            messagebox.showerror('Not saved', 'Local configuration failed. Credential values have not been displayed or logged.', parent=root)

    buttons = tk.Frame(outer, bg='#101a2b'); buttons.pack(fill='x', pady=(6, 0))
    tk.Button(buttons, text='Save locally', command=save, bg='#e7bc63', fg='#142034',
              relief='flat', borderwidth=0, padx=28, pady=10, cursor='hand2').pack(side='left')
    tk.Button(buttons, text='Close', command=lambda: (clear(), root.destroy()), bg='#26354a',
              fg='#dce7f5', relief='flat', padx=22, pady=10).pack(side='right')
    tk.Label(outer, text='Saving does not verify exchange permissions or remove account restrictions.',
             fg='#93a6bf', bg='#101a2b', wraplength=570, justify='left').pack(anchor='w', pady=(22, 0))
    root.protocol('WM_DELETE_WINDOW', lambda: (clear(), root.destroy()))
    root.mainloop()


if __name__ == '__main__':
    main()
