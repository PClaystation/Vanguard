const revealItems = document.querySelectorAll(".reveal");
const tiltTargets = document.querySelectorAll("[data-tilt]");

const revealObserver = new IntersectionObserver(
  (entries) => {
    for (const entry of entries) {
      if (entry.isIntersecting) {
        entry.target.classList.add("visible");
      }
    }
  },
  { threshold: 0.15 }
);

for (const item of revealItems) {
  revealObserver.observe(item);
}

for (const target of tiltTargets) {
  const intensity = 6;

  target.addEventListener("pointermove", (event) => {
    const rect = target.getBoundingClientRect();
    const px = (event.clientX - rect.left) / rect.width;
    const py = (event.clientY - rect.top) / rect.height;
    const rotateY = (px - 0.5) * intensity;
    const rotateX = (0.5 - py) * intensity;
    target.style.transform = `perspective(1000px) rotateX(${rotateX}deg) rotateY(${rotateY}deg)`;
  });

  target.addEventListener("pointerleave", () => {
    target.style.transform = "";
  });
}
