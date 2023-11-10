import keyboard
import time

time.sleep(5)
i = 0
while i <= 50:
    keyboard.write('Элина ван лав')

    time.sleep(0.05)

    keyboard.press_and_release('enter')

    time.sleep(0.05)

    i +=1
