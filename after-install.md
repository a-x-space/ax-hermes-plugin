# ax Hermes plugin installed

Enable and bind it:

```bash
hermes plugins enable ax
hermes ax bind
hermes gateway restart
```

The bind command prints a code and an ax approval URL. Approve the code in ax,
then restart the Hermes gateway so the platform connection comes online.
